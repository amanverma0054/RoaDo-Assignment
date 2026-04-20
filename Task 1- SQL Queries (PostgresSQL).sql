SELECT current_database();

-- Q1: Plan-level metrics for last 6 months

WITH last_6_months AS (
    SELECT *
    FROM nimbus.subscriptions
    WHERE start_date >= CURRENT_DATE - INTERVAL '6 months'
),

tickets_6m AS (
    SELECT customer_id,
           COUNT(*) AS total_tickets
    FROM nimbus.support_tickets
    WHERE created_at >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY customer_id
)

SELECT 
    p.plan_name,
    COUNT(DISTINCT s.customer_id) AS active_customers,
    
    AVG(p.monthly_price_usd) AS avg_monthly_revenue,

    -- tickets per customer per month
    ROUND(
        SUM(COALESCE(t.total_tickets, 0))::DECIMAL 
        / NULLIF(COUNT(DISTINCT s.customer_id) * 6, 0),
    2) AS ticket_rate_per_customer_per_month

FROM last_6_months s
JOIN nimbus.plans p ON s.plan_id = p.plan_id
LEFT JOIN tickets_6m t ON s.customer_id = t.customer_id

WHERE s.status = 'active'

GROUP BY p.plan_name
ORDER BY p.plan_name;


-- Q2: Rank customers by LTV within each plan tier

WITH customer_ltv AS (
    SELECT 
        c.customer_id,
        p.plan_tier,
        SUM(p.monthly_price_usd) AS ltv
    FROM nimbus.subscriptions s
    JOIN nimbus.customers c ON s.customer_id = c.customer_id
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    GROUP BY c.customer_id, p.plan_tier
),

tier_avg AS (
    SELECT 
        plan_tier,
        AVG(ltv) AS avg_ltv
    FROM customer_ltv
    GROUP BY plan_tier
)

SELECT 
    cl.customer_id,
    cl.plan_tier,
    cl.ltv,

    RANK() OVER (
        PARTITION BY cl.plan_tier 
        ORDER BY cl.ltv DESC
    ) AS rank_in_tier,

    ROUND(
        (cl.ltv - ta.avg_ltv) * 100.0 
        / NULLIF(ta.avg_ltv, 0),
        2
    ) AS pct_diff_from_avg

FROM customer_ltv cl
JOIN tier_avg ta 
    ON cl.plan_tier = ta.plan_tier;


-- Q3: Customers who downgraded in last 90 days + had >3 tickets before downgrade


WITH plan_changes AS (
    SELECT 
        s.customer_id,
        s.plan_id,
        s.start_date,
        LAG(s.plan_id) OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.start_date
        ) AS prev_plan_id,
        LAG(s.start_date) OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.start_date
        ) AS prev_start_date
    FROM nimbus.subscriptions s
),

downgrades AS (
    SELECT 
        pc.customer_id,
        pc.plan_id AS current_plan_id,
        pc.prev_plan_id,
        pc.start_date,
        pc.prev_start_date
    FROM plan_changes pc
    JOIN nimbus.plans curr 
        ON pc.plan_id = curr.plan_id
    JOIN nimbus.plans prev 
        ON pc.prev_plan_id = prev.plan_id
    WHERE curr.monthly_price_usd < prev.monthly_price_usd
      AND pc.start_date >= CURRENT_DATE - INTERVAL '90 days'
),

ticket_check AS (
    SELECT 
        d.customer_id,
        d.start_date,
        COUNT(t.ticket_id) AS ticket_count
    FROM downgrades d
    JOIN nimbus.support_tickets t 
        ON t.customer_id = d.customer_id
       AND t.created_at BETWEEN d.prev_start_date AND d.start_date
    GROUP BY d.customer_id, d.start_date
)

SELECT 
    d.customer_id,
    prev.plan_name AS previous_plan,
    curr.plan_name AS current_plan,
    tc.ticket_count

FROM downgrades d
JOIN ticket_check tc 
    ON d.customer_id = tc.customer_id
   AND d.start_date = tc.start_date

JOIN nimbus.plans curr 
    ON d.current_plan_id = curr.plan_id

JOIN nimbus.plans prev 
    ON d.prev_plan_id = prev.plan_id

WHERE tc.ticket_count > 3;


-- Q4: MoM growth + rolling churn


WITH monthly_data AS (
    SELECT 
        DATE_TRUNC('month', start_date) AS month,
        plan_id,
        COUNT(*) FILTER (WHERE status = 'active') AS new_subs,
        COUNT(*) FILTER (WHERE status = 'churned') AS churns
    FROM nimbus.subscriptions
    GROUP BY 1,2
),

calc AS (
    SELECT 
        m.*,
        LAG(new_subs) OVER (
            PARTITION BY plan_id 
            ORDER BY month
        ) AS prev_month_subs
    FROM monthly_data m
),

final AS (
    SELECT 
        c.*,

        -- MoM growth
        (new_subs - prev_month_subs) * 1.0 
        / NULLIF(prev_month_subs, 0) AS mom_growth,

        -- rolling churn avg
        AVG(churns) OVER (
            PARTITION BY plan_id 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_churn_avg

    FROM calc c
)

SELECT 
    f.*,
    CASE 
        WHEN churns > 2 * rolling_churn_avg THEN 'FLAG'
        ELSE 'OK'
    END AS churn_flag
FROM final f;


-- Q5: Detect duplicate customers
-- Logic:
-- 1. Similar company names (LOWER + LIKE)
-- 2. Same email domain
-- 3. Shared team members


WITH email_domain AS (
    SELECT 
        customer_id,
        LOWER(SPLIT_PART(contact_email, '@', 2)) AS domain
    FROM nimbus.customers
),

potential_duplicates AS (
    SELECT 
        c1.customer_id AS customer_1,
        c2.customer_id AS customer_2,
        
        -- name similarity
        c1.company_name,
        c2.company_name,

        -- domain match
        e1.domain

    FROM nimbus.customers c1
    JOIN nimbus.customers c2 
        ON c1.customer_id < c2.customer_id

    JOIN email_domain e1 
        ON c1.customer_id = e1.customer_id
    JOIN email_domain e2 
        ON c2.customer_id = e2.customer_id

    WHERE 
        LOWER(c1.company_name) LIKE '%' || LOWER(c2.company_name) || '%'
        OR e1.domain = e2.domain
)

SELECT * 
FROM potential_duplicates;