const { onRequest } = require("firebase-functions/v2/https");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore, FieldValue } = require("firebase-admin/firestore");

initializeApp();
const db = getFirestore();

// ─── Config ───
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";

const APP_SLUGS = {
  "christian-daily-task": [
    "com.nicholasseither.christiandailytask",
    "cdt",
    "christian_daily_task",
    "christian-daily-task",
  ],
  girlwalk: ["com.tangentapps.girlwalk", "girlwalk", "girl_walk"],
  girltalk: ["com.tangentapps.girltalk", "girltalk", "girl_talk"],
};

// Resolve a product_id or app_user_id hint to an app slug
function resolveApp(productId, appSlugHint) {
  // If the URL path already tells us which app
  if (appSlugHint && APP_SLUGS[appSlugHint]) return appSlugHint;

  // Try matching by product_id prefix
  const pid = (productId || "").toLowerCase();
  for (const [slug, prefixes] of Object.entries(APP_SLUGS)) {
    if (prefixes.some((p) => pid.includes(p))) return slug;
  }
  return null;
}

// ─── Classification (mirrors dashboard logic) ───
// Determine the user's trial exit status from the event type
function classifyFromEvent(eventType, cancelReason, periodType) {
  switch (eventType) {
    case "INITIAL_PURCHASE":
      return periodType === "TRIAL" ? "Still in Trial" : "Converted";

    case "RENEWAL":
      // A renewal after a trial = conversion
      return "Converted";

    case "CANCELLATION":
      if (
        cancelReason === "BILLING_ERROR" ||
        cancelReason === "CUSTOMER_SUPPORT"
      ) {
        return "Billing Issue";
      }
      return "Cancelled";

    case "BILLING_ISSUE":
      return "Billing Issue";

    case "EXPIRATION":
      // Trial expired without converting
      return "Cancelled";

    case "UNCANCELLATION":
      return "Still in Trial";

    default:
      return null; // Unknown event, don't update
  }
}

// Get UTC date string from ms timestamp
function msToDateKey(ms) {
  if (!ms) return null;
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// ─── Backfill handler (accepts processed cohort data from dashboard) ───
exports.backfill = onRequest(
  { region: "us-central1", cors: true },
  async (req, res) => {
    if (req.method !== "POST") {
      return res.status(405).send("Method not allowed");
    }

    try {
      const { slug, cohorts } = req.body;
      if (!slug || !Array.isArray(cohorts) || cohorts.length === 0) {
        return res.status(400).json({ status: "error", reason: "missing slug or cohorts array" });
      }
      if (!APP_SLUGS[slug]) {
        return res.status(400).json({ status: "error", reason: "unknown app slug" });
      }

      // Batch write cohort documents (max 500 per batch)
      const batches = [];
      let batch = db.batch();
      let count = 0;

      for (const c of cohorts) {
        if (!c.period) continue;
        const ref = db.doc(`apps/${slug}/cohorts/${c.period}`);
        batch.set(ref, {
          period: c.period,
          total_trials: c.total_trials || 0,
          in_trial: c.in_trial || 0,
          converted: c.converted || 0,
          cancelled: c.cancelled || 0,
          billing_issue: c.billing_issue || 0,
          conversion_rate: c.conversion_rate || 0,
          cancel_rate: c.cancel_rate || 0,
          billing_rate: c.billing_rate || 0,
        });
        count++;
        if (count % 450 === 0) {
          batches.push(batch);
          batch = db.batch();
        }
      }
      batches.push(batch);

      await Promise.all(batches.map((b) => b.commit()));
      console.log(`Backfilled ${count} cohorts for ${slug}`);
      return res.status(200).json({ status: "ok", count });
    } catch (err) {
      console.error("Backfill error:", err);
      return res.status(500).json({ status: "error", message: err.message });
    }
  }
);

// ─── Webhook handler ───
exports.webhook = onRequest(
  { region: "us-central1", cors: false },
  async (req, res) => {
    // Only accept POST
    if (req.method !== "POST") {
      return res.status(405).send("Method not allowed");
    }

    // Verify authorization header if secret is configured
    if (WEBHOOK_SECRET) {
      const auth = req.headers["authorization"] || "";
      if (auth !== `Bearer ${WEBHOOK_SECRET}`) {
        console.warn("Unauthorized webhook attempt");
        return res.status(401).send("Unauthorized");
      }
    }

    try {
      const body = req.body;
      const event = body.event || body;

      const eventType = event.type;
      const appUserId =
        event.app_user_id || event.original_app_user_id || null;
      const productId = event.product_id || "";
      const periodType = event.period_type || "";
      const cancelReason = event.cancel_reason || "";
      const purchasedAtMs = event.purchased_at_ms || null;
      const environment = event.environment || "PRODUCTION";

      // Skip sandbox events
      if (environment === "SANDBOX") {
        return res.status(200).json({ status: "skipped", reason: "sandbox" });
      }

      if (!appUserId) {
        return res
          .status(400)
          .json({ status: "error", reason: "missing app_user_id" });
      }

      // Resolve which app this belongs to (from URL path or product_id)
      const pathSlug = req.path.replace(/^\//, "").split("/")[0] || null;
      const appSlug = resolveApp(productId, pathSlug);
      if (!appSlug) {
        console.warn(
          `Could not resolve app for product: ${productId}, path: ${pathSlug}`
        );
        return res
          .status(200)
          .json({ status: "skipped", reason: "unknown_app" });
      }

      // Classify the user's current status
      const status = classifyFromEvent(eventType, cancelReason, periodType);
      if (!status) {
        return res
          .status(200)
          .json({ status: "skipped", reason: "unhandled_event_type" });
      }

      // Determine trial start date for cohort bucketing
      const trialDate = msToDateKey(purchasedAtMs);

      // ─── Update user doc ───
      const userRef = db.doc(`apps/${appSlug}/users/${appUserId}`);
      const userSnap = await userRef.get();
      const prevStatus = userSnap.exists ? userSnap.data().status : null;
      const trialStartDate = userSnap.exists
        ? userSnap.data().trial_start_date || trialDate
        : trialDate;

      await userRef.set(
        {
          status,
          product: productId,
          trial_start_date: trialStartDate,
          last_event: eventType,
          updated_at: FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      // ─── Update daily cohort doc ───
      if (trialStartDate) {
        const cohortRef = db.doc(
          `apps/${appSlug}/cohorts/${trialStartDate}`
        );

        await db.runTransaction(async (tx) => {
          const cohortSnap = await tx.get(cohortRef);
          const data = cohortSnap.exists
            ? cohortSnap.data()
            : {
                period: trialStartDate,
                total_trials: 0,
                in_trial: 0,
                converted: 0,
                cancelled: 0,
                billing_issue: 0,
              };

          // If this is a brand new user (INITIAL_PURCHASE), increment total
          if (eventType === "INITIAL_PURCHASE" && !prevStatus) {
            data.total_trials += 1;
          }

          // Decrement previous status bucket
          if (prevStatus === "Still in Trial") data.in_trial = Math.max(0, data.in_trial - 1);
          else if (prevStatus === "Converted") data.converted = Math.max(0, data.converted - 1);
          else if (prevStatus === "Cancelled") data.cancelled = Math.max(0, data.cancelled - 1);
          else if (prevStatus === "Billing Issue") data.billing_issue = Math.max(0, data.billing_issue - 1);

          // Increment new status bucket
          if (status === "Still in Trial") data.in_trial += 1;
          else if (status === "Converted") data.converted += 1;
          else if (status === "Cancelled") data.cancelled += 1;
          else if (status === "Billing Issue") data.billing_issue += 1;

          // Compute rates
          const t = data.total_trials || 1;
          data.conversion_rate = Math.round((data.converted / t) * 10000) / 10000;
          data.cancel_rate = Math.round((data.cancelled / t) * 10000) / 10000;
          data.billing_rate = Math.round((data.billing_issue / t) * 10000) / 10000;

          tx.set(cohortRef, data);
        });
      }

      console.log(
        `Processed ${eventType} for ${appUserId} → ${status} (${appSlug})`
      );
      return res.status(200).json({ status: "ok", app: appSlug, classification: status });
    } catch (err) {
      console.error("Webhook error:", err);
      return res.status(500).json({ status: "error", message: err.message });
    }
  }
);
