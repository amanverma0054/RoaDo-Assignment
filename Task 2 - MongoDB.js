// analytics_assignment.js
// MongoDB Analytics Queries Assignment

// Connect to MongoDB (adjust DB name if needed)
const { MongoClient } = require("mongodb");

const uri = "mongodb://localhost:27017"; // change if using Atlas
const client = new MongoClient(uri);

async function runQueries() {
  try {
    await client.connect();
    const db = client.db("nimbus_events");

    // -------------------------------
    // Q1: Avg sessions per user per week + percentiles
    // -------------------------------
    const q1 = await db.collection("sessions").aggregate([
      {
        $group: {
          _id: {
            user_id: "$user_id",
            week: { $week: "$session_start" }
          },
          session_count: { $sum: 1 },
          durations: { $push: "$duration_seconds" }
        }
      },
      {
        $group: {
          _id: "$_id.user_id",
          avg_sessions_per_week: { $avg: "$session_count" },
          all_durations: { $push: "$durations" }
        }
      },
      { $unwind: "$all_durations" },
      { $unwind: "$all_durations" },
      {
        $group: {
          _id: null,
          p25: { $percentile: { input: "$all_durations", p: [0.25] } },
          p50: { $percentile: { input: "$all_durations", p: [0.5] } },
          p75: { $percentile: { input: "$all_durations", p: [0.75] } }
        }
      }
    ]).toArray();

    console.log("Q1 Result:", q1);

    // -------------------------------
    // Q2: DAU (Daily Active Users)
    // -------------------------------
    const q2 = await db.collection("events").aggregate([
      {
        $match: { event_type: "feature_use" }
      },
      {
        $group: {
          _id: {
            feature: "$feature_name",
            date: {
              $dateToString: { format: "%Y-%m-%d", date: "$timestamp" }
            }
          },
          users: { $addToSet: "$user_id" }
        }
      },
      {
        $project: {
          feature: "$_id.feature",
          date: "$_id.date",
          dau: { $size: "$users" }
        }
      }
    ]).toArray();

    console.log("Q2 Result:", q2);

    // -------------------------------
    // Q3: Funnel stages per user
    // -------------------------------
    const q3 = await db.collection("events").aggregate([
      {
        $match: {
          event_type: {
            $in: [
              "signup",
              "first_login",
              "workspace_created",
              "first_project",
              "invited_teammate"
            ]
          }
        }
      },
      {
        $group: {
          _id: "$user_id",
          events: {
            $push: {
              type: "$event_type",
              time: "$timestamp"
            }
          }
        }
      }
    ]).toArray();

    console.log("Q3 Result:", q3);

    // -------------------------------
    // Q4: Engagement score
    // -------------------------------
    const q4 = await db.collection("events").aggregate([
      {
        $group: {
          _id: "$user_id",
          total_events: { $sum: 1 },
          active_days: {
            $addToSet: {
              $dateToString: {
                format: "%Y-%m-%d",
                date: "$timestamp"
              }
            }
          }
        }
      },
      {
        $project: {
          engagement_score: {
            $add: [
              "$total_events",
              { $multiply: [{ $size: "$active_days" }, 5] }
            ]
          }
        }
      },
      { $sort: { engagement_score: -1 } },
      { $limit: 20 }
    ]).toArray();

    console.log("Q4 Result:", q4);

  } catch (err) {
    console.error("Error running queries:", err);
  } finally {
    await client.close();
  }
}

runQueries();