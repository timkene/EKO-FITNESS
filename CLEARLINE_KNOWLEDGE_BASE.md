# CLEARLINE INTERNATIONAL LIMITED - Knowledge Base

**Company:** CLEARLINE INTERNATIONAL LIMITED  
**AI Analyst:** KLAIRE  
**Last Updated:** 2026-05-05 (Updated with total medical cost/MLR date rules and pipeline safeguards)

## Business Model

Clearline International Limited is a health insurance company (HMO) that:
- Partners with hospitals (providers)
- Takes premiums from customers (groups/clients)
- Provides health insurance services to enrollees
- Sells plans (individual or family) to clients

## Key Concepts

### Clients/Groups
- **Groups** = **Clients** = **groupname** (all refer to the same thing)
- Each client has a unique `groupid`
- Use `GROUPS` or `ALL_GROUPS` table to get client name from `groupid`
- Some tables already have `groupname` (like PA DATA), making it easier

### Plans
- Plans can be **Individual** or **Family** (found in `GROUP_PLAN` table)
- Each plan has:
  - Cost per unit
  - Number of units sold (individual/family/both)
- Clients can have multiple plans (up to 10 plans per client)
- Plans are sold for **1 year periods**, then must be renewed
- `GROUP_CONTRACT` contains contract periods for each client
- Always use contract dates unless user specifies a different period

### Enrollees/Members
- **Enrollees** = **Members** = **Customers**
- IDs can appear as: `legacycode`, `enrollee_id`, `IID`, `memberid` (case insensitive)
- If you see both `memberid` and `legacycode` in same table, use them to map and get member info
- `ALL_ACTIVE_MEMBER` contains all active enrollees with DOB, phone, email, gender, etc.
- `MEMBER` / `MEMBERS` contains **all members ever enrolled**, including terminated ones  
  - Use this when a claim or PA refers to an enrollee that is no longer active (terminated)  
  - From here you can get historical details: when the member was added, date of birth, gender, contact info, etc.
- `MEMBER_COVERAGE` (raw coverage periods) contains all historical coverage records for members  
  - Use this to see when coverage started/ended for a member, even after termination  
  - This complements `ALL_ACTIVE_MEMBER`, which only shows current, non‑terminated coverage
- `MEMBER_PLAN` contains members and their plans - use `iscurrent = 1` for current plan
- One member can have multiple plans - always use `iscurrent = 1` for current data

## Providers & Tariffs

### Providers (Hospitals)
- `PROVIDERS` or `ALL_PROVIDERS` contains all hospitals we partner with
- Each provider has unique `providerid`
- Use `providerid` to get provider name from `ALL_PROVIDERS`

### Tariffs
- Tariffs are price lists containing procedures and agreed prices
- Each provider is mapped to a tariff (found in `PROVIDERS_TARIFF`)
- `TARIFF` table contains all tariffs with procedures and prices
- Use `tariffid` to get `tariffname` from `TARIFF` table
- Providers use their mapped tariff prices for transactions

## Pre-Authorization (PA) Process

### PA DATA Table
- When enrollee goes to hospital, hospital sends PA request
- Request contains: diagnosis and procedures to be performed
- Clearline gives authorization code called `panumber`
- `panumber` is unique to enrollee + date (in `requestdate` column)
- `granted` column contains the authorized amount
- `procedurecode` or `code` = unique alphanumeric code for each procedure
- Use `procedurecode` to get procedure name from `PROCEDUREDATA` table

### Diagnosis
- `TBPADIAGNOSIS` table contains all `panumber` and their diagnosis
- Use `TBPADIAGNOSIS` to get diagnosis for a `panumber`
- `DIAGNOSIS` table (from MediCloud) contains diagnosis codes and descriptions
- Use diagnosis code to get diagnosis description from `DIAGNOSIS` table

## Claims Process

### CLAIMS DATA Table
- After PA is given, hospital submits claim for vetting and payment
- **Two important dates:**
  - `encounterdatefrom` = date the encounter took place (usually same as PA `requestdate`, but not always)
  - `datesubmitted` = date the claim was submitted
- **When asked for claims amount, provide for BOTH dates**
- **Important:** Claims can exist WITHOUT `panumber` - this is normal!
  - Some authorization is done at hospital end to reduce delay
  - Rows without `panumber` are allowed and expected
- `approvedamount` = amount approved for payment
- `deniedamount` = amount denied from that transaction
- `chargeamount` = amount submitted by hospital
- `procedurecode` = procedure code used
- `diagnosiscode` = diagnosis code (use this to get diagnosis description from DIAGNOSIS table)
- For total medical cost / MLR by utilization period, use `encounterdatefrom` as the primary claims date.
- Use `datesubmitted` only when the user explicitly asks for submission-based analysis.

## Benefits & Limits

### Benefits
- Benefits are classes/buckets of procedures
- Relationship: `BENEFITCODE_PROCEDURES` maps `procedurecode` → `benefitcodeid`
- `BENEFITCODES` contains benefit descriptions
- To get benefit for a procedure:
  1. Get `benefitcodeid` from `BENEFITCODE_PROCEDURES` using `procedurecode`
  2. Get benefit description from `BENEFITCODES` using `benefitcodeid`

### Plan Benefit Limits

#### PLANBENEFITCODE_LIMIT Table
- **Purpose**: Defines limits for each plan × benefit combination
- **Key Columns**:
  - `planid`: Links to PLANS table
  - `benefitcodeid`: Links to BENEFITCODES table
  - `maxlimit`: Monetary limit (e.g., ₦200,000) - NULL if no monetary limit
  - `countperannum`: Count limit per year (e.g., 15 times) - NULL if no count limit
  - `countperweek`, `countperquarter`, `countpertwoyears`, `countperlifetime`: Other count limits
  - `daysallowed`: Days allowed for certain benefits (e.g., inpatient days)
- **Limit Types**:
  - **Monetary Limits** (`maxlimit`): Maximum amount that can be spent on a benefit per period
  - **Count Limits** (`countperannum`, etc.): Maximum number of times a benefit can be used per period
  - **Unlimited Benefits**: Benefits with NO limit defined (NULL for both `maxlimit` and `countperannum`) - these represent exposure risks

#### Benefit Limit Analysis Use Cases

**1. Over-Limit Members (Fraud/Abuse Detection)**
- Identify members who have exceeded their benefit limits
- Check: Sum `approvedamount` (claims) + `granted` (unclaimed PA) for monetary limits, or COUNT(*) for count limits
- Compare against `maxlimit` or `countperannum` in PLANBENEFITCODE_LIMIT
- Risk Levels: 🔴 CRITICAL (>100% over limit), 🟡 MODERATE (<100% over)

**2. Unlimited Benefits (Exposure Risk)**
- Identify benefits that are being used but have NO limits defined
- Risk Levels: 🔴 HIGH EXPOSURE (>₦5M spent), 🟡 MODERATE EXPOSURE (>₦1M spent), ✅ LOW EXPOSURE (<₦1M spent)
- Recommendation: Add limits for high/moderate exposure benefits

**3. Benefit Cost Breakdown**
- Understand which benefits are driving costs for a company
- Report: Benefit name, unique members using, total times used, total cost, average cost per use, percentage of total medical cost
- Order by total cost DESC (top cost drivers first)

**4. High-Cost Members per Benefit**
- Identify member concentration risk (few members driving high costs)
- Risk Levels: 🔴 CRITICAL (>₦10M), 🟡 HIGH (>₦5M), ✅ MODERATE

#### How to Check if Enrollee Exceeded Limits
1. Get member's plan: Use `MEMBER_PLAN` with `iscurrent = 1` to get `planid`
2. Get member's utilization: Combine claims (`CLAIMS DATA`) and unclaimed PA (`PA DATA`) for contract period
3. Map procedures to benefits: Use `BENEFITCODE_PROCEDURES` to get `benefitcodeid`
4. Aggregate by benefit: SUM amounts for monetary, COUNT for count limits
5. Compare against limits: Join with `PLANBENEFITCODE_LIMIT` using `planid` + `benefitcodeid`
6. Flag overages: Calculate `total_spent - maxlimit` or `times_used - countperannum`

#### Important Notes
- Always filter utilization by contract dates from `GROUP_CONTRACT`
- Include both claims AND unclaimed PA for current contracts
- Use `MEMBER_PLAN` with `iscurrent = 1` to get correct plan
- A benefit can have BOTH monetary AND count limits - check both separately
- Use `LOWER(TRIM())` when matching procedure codes between tables
- A benefit with `maxlimit IS NULL` AND `countperannum IS NULL` is UNLIMITED

### Plans Mapping
- `PLANS` table maps `plancode` ↔ `planid`
- Use this when you see `plancode` in some tables and `planid` in others

## Coverage & Contracts

### GROUP_CONTRACT
- Contains contract periods for each client
- Contracts are always **1 year periods**
- Always use contract dates when doing client analysis unless user specifies otherwise

### GROUP_COVERAGE
- Clients can pay in installments
- Coverage extends as payments are made
- Coverage periods change with payments
- **Important:** Coverage should NEVER be outside contract period
- When coverage hits 1 year, it restarts if client renews
- Use `GROUP_COVERAGE` to find clients that will be terminated this month by checking `enddate`

## Table Relationships Summary

### Key Relationships
- `PA DATA.panumber` → `CLAIMS DATA.panumber` (PA to claims linkage)
- `PA DATA.providerid` → `PROVIDERS.providerid` (PA to provider)
- `CLAIMS DATA.enrollee_id` → `MEMBERS.enrollee_id` (Claims to members)
- `MEMBERS.groupid` → `GROUPS.groupid` (Members to groups)
- `CLAIMS DATA.panumber` → `TBPADIAGNOSIS.panumber` (Claims to diagnoses)
- `PA DATA.code` → `PROCEDURE DATA.procedurecode` (PA to procedure details)
- `PROVIDERS_TARIFF.providerid` → `PROVIDERS.providerid` (Provider to tariff mapping)
- `PROVIDERS_TARIFF.tariffid` → `TARIFF.tariffid` (Tariff details)
- `BENEFITCODE_PROCEDURES.procedurecode` → `PROCEDURE DATA.procedurecode`
- `BENEFITCODE_PROCEDURES.benefitcodeid` → `BENEFITCODES.benefitcodeid`
- `PLANBENEFITCODE_LIMIT.planid` → `PLANS.planid`
- `PLANBENEFITCODE_LIMIT.benefitcodeid` → `BENEFITCODES.benefitcodeid`
- `MEMBER_PLAN.memberid` → `MEMBERS.memberid`
- `MEMBER_PLAN.planid` → `PLANS.planid`

## Important Rules

1. **Always use contract dates** from `GROUP_CONTRACT` for client analysis unless user specifies otherwise
2. **Use `iscurrent = 1`** for current data where applicable (MEMBER_PLAN, GROUP_COVERAGE, etc.)
3. **Claims without panumber are normal** - don't filter them out
4. **Provide claims data for BOTH dates** - `encounterdatefrom` and `datesubmitted`
5. **Enrollee IDs vary** - use `legacycode`, `enrollee_id`, `IID`, or `memberid` (case insensitive)
6. **One member can have multiple plans** - always use `iscurrent = 1`
7. **Coverage never exceeds contract period** - important for termination checks
8. **Always ask if unclear** - don't assume

## Medical Loss Ratio (MLR)

### Definition
- **MLR** = Medical Loss Ratio
- **Calculation**: Percentage of money received by clients that was used for medical expenses
- **Formula**: MLR = (Total Medical Spending / Total Premium Received) × 100%

### Total Medical Spending Calculation
- **Total Medical Spending** = Claims + Unclaimed PA
  - **Claims**: Sum `approvedamount` from CLAIMS DATA
    - Default date for spending/utilization analysis: `encounterdatefrom`
    - Use `datesubmitted` only when explicitly requested
  - **Unclaimed PA**: Sum `granted` from PA DATA where PA has not yet been claimed
    - Build PA panumber set from PA DATA for the same scope and period
    - Build claims panumber set from CLAIMS DATA for the same scope and period
    - Unclaimed PA panumbers = PA panumbers not present in the claims panumber set
- This gives the total amount spent and authorized-but-not-yet-claimed for the same period.

### MLR Calculation Period
- Can be calculated for:
  - **Current contract period**: Use GROUP_CONTRACT dates
  - **Stated period**: Use user-specified dates
- Compare against CLIENT_CASH_RECEIVED for the same period to get accurate MLR
- Always apply the same period window consistently across claims and unclaimed PA.

## Financial Tables

### Derived Tables

#### CLIENT_CASH_RECEIVED
- **Purpose**: Tracks cash received from each client with date
- **Use Case**: 
  - Compare cash received vs. claims paid + unclaimed PA for current contract or stated period
  - Calculate MLR: (Claims + Unclaimed PA) / Cash Received
- **Structure**: Contains client code, date, amount received
- **Matching Logic**: Matches transactions on code + refno + date

#### SALARY_AND_PALLIATIVE
- **Purpose**: Contains all salary paid month by month to Clearline staff
- **Structure**: Monthly salary transactions
- **Matching Logic**: Matches on refno + date (no company code needed - this is internal)

#### EXPENSE_AND_COMMISSION
- **Purpose**: Contains all Clearline operational expenses including commissions
- **Structure**: Cost of each type of expense (operational costs, commissions, etc.)
- **Matching Logic**: Matches on gldesc + date

#### DEBIT_NOTE_ACCRUED
- **Purpose**: Contains all debit cost spread into months (allocated monthly amounts)
- **Structure**: Shows amount allocated per month to be spent on each company from start date to end date
- **Use Case**: 
  - Get monthly MLR by comparing monthly allocated amounts vs. monthly medical spending (claims + unclaimed PA)
  - Track monthly allocation vs. actual spending per client
- **Matching Logic**: Matches on code + date

### FIN_GL Raw Tables
- **FIN_GL_2023_RAW**: GL data from Excel (sheet 1)
- **FIN_GL_2024_RAW**: GL data from Excel (sheet 2)
- **FIN_GL_2025_RAW**: GL data from EACCOUNT database

## Column Naming Notes

- `legacycode`, `enrollee_id`, `IID` = enrollee ID (case insensitive)
- `memberid` = member ID
- `panumber` = PA authorization code
- `groupid` = client/group unique ID
- `providerid` = hospital/provider unique ID
- `tariffid` = tariff unique ID
- `procedurecode` or `code` = procedure unique code
- `benefitcodeid` = benefit unique ID
- `planid` = plan unique ID
- `plancode` = plan code (can map to planid via PLANS table)

## Data Updates

- All tables update via `auto_update_database.py`
- Derived tables rebuild automatically after source tables update
- GROUP_CONTRACT must be loaded for accurate contract date queries
- Always verify table exists before querying
- Safeguard: if a source fetch is empty due to connection issues, do not overwrite populated production tables with sample rows.

## Common Query Patterns

**Note:** See `KLAIRE_QUERY_TEMPLATES.md` for detailed SQL query templates for these patterns.

### 1. Medical History Analysis (Enrollee Medical Records)

**User Query Pattern:**
- "show me all medical record for [ENROLLEE_ID] for the last [PERIOD]"
- "show me all medical history for [ENROLLEE_ID] for the past [PERIOD]"

**What to Provide:**
1. **All Prior Authorizations (PAs)** with:
   - PA number, request date, status, provider name
   - All procedures in each PA (procedure code, description, requested amount, granted amount)
   - All diagnoses for each PA (diagnosis code, description)
   
2. **All Claims** with:
   - Claim number, submission date, encounter date
   - Procedures claimed (procedure code, charge amount, approved amount, denied amount)
   - Diagnoses associated with claims
   
3. **Medical Trends & Insights:**
   - **Frequent conditions**: Count occurrences of each diagnosis (e.g., "Malaria treated 5 times")
   - **Frequent procedures**: Count occurrences of each procedure type
   - **Antibiotic usage**: Flag frequent antibiotic prescriptions (amoxicillin, azithromycin, ceftriaxone, ciprofloxacin, metronidazole, etc.)
   - **Antimalaria usage**: Flag frequent antimalaria drug usage (artemether, lumefantrine, artesunate, etc.)
   - **Pain management**: Track pain medication usage (paracetamol, diclofenac, ibuprofen, tramadol, etc.)
   - **Chronic conditions**: Identify chronic disease management (hypertension, diabetes, asthma, COPD, heart conditions)
   - **Surgical procedures**: Identify surgeries vs. medical treatments
   - **Dental procedures**: Track dental visits and procedures
   - **Provider patterns**: Which hospitals are used most frequently
   - **Cost trends**: Total costs over time, average cost per visit
   - **Gaps in care**: Missing follow-ups, incomplete treatments
   - **Red flags**: Co-infections, severe conditions, unusual patterns

**Key Tables:**
- `PA DATA` - Prior authorizations (use `IID`, `enrollee_id`, or `memberid` to find member)
- `TBPADIAGNOSIS` - Diagnoses for each PA (link via `panumber`)
- `DIAGNOSIS` - Diagnosis descriptions (link via diagnosis code)
- `PROCEDURE DATA` - Procedure descriptions (link via procedure code)
- `CLAIMS DATA` - Claims submitted (link via `panumber` or `enrollee_id`)
- `PROVIDERS` - Hospital/provider names (link via `providerid`)

**Date Filtering:**
- Use `requestdate` from PA DATA for PA filtering
- Use `datesubmitted` and `encounterdatefrom` from CLAIMS DATA for claims filtering
- Support periods: "last 3 months", "last 6 months", "last 1 year", "last 2 years", or specific date ranges

### 2. MLR Comparison Between Contract Periods

**User Query Pattern:**
- "compare [COMPANY_NAME] mlr between last contract and this one"
- "compare [COMPANY_NAME] mlr between last contract and current contract"
- "why is [COMPANY_NAME] mlr good/bad this year compared to last year"

**What to Provide:**

1. **MLR Comparison:**
   - Last contract MLR vs. Current contract MLR
   - Premium received (cash received) for both periods
   - Medical spending (claims + unclaimed PA) for both periods
   - MLR percentage change and direction (improving or worsening)

2. **Hospital/Provider Analysis:**
   - **New hospitals**: Hospitals used in current contract but not in last contract
   - **Dropped hospitals**: Hospitals used in last contract but not in current contract
   - **Hospital cost changes**: Compare costs per hospital between periods
   - **Top cost drivers**: Which hospitals are driving the MLR change (good or bad)
   - **Hospital utilization**: Number of visits/cases per hospital in each period
   - **Average cost per visit**: Compare hospital pricing between periods

3. **Diagnosis Analysis:**
   - **New diagnoses**: Diagnoses appearing in current contract but not in last contract
   - **Diagnosis pattern changes**: Shifts in diagnosis frequency (e.g., more malaria, more surgeries)
   - **Top cost diagnoses**: Which diagnoses are costing most in each period
   - **Severity changes**: More severe conditions in current vs. last contract

4. **Procedure Analysis:**
   - **Surgical procedures**: More/fewer surgeries in current contract
   - **Dental procedures**: More/fewer dental visits in current contract
   - **Specialty procedures**: Changes in specialty care (cardiology, orthopedics, etc.)
   - **Procedure cost changes**: Price changes for common procedures

5. **Utilization Analysis:**
   - **Total cases**: Number of PAs/claims in each period
   - **Average cost per case**: Compare between periods
   - **Member utilization**: More/fewer members using services
   - **Frequency patterns**: More frequent visits per member

6. **Root Cause Summary:**
   - Summarize the main drivers of MLR change
   - Identify if it's hospital-related, diagnosis-related, procedure-related, or utilization-related
   - Highlight significant findings (e.g., "New expensive hospital added", "More surgeries this year", "Malaria cases doubled")

**Key Tables:**
- `GROUP_CONTRACT` - Contract periods (use `iscurrent = 1` for current, `iscurrent = 0` for last contract)
- `CLIENT_CASH_RECEIVED` - Premium received (filter by contract dates)
- `CLAIMS DATA` - Claims (filter by `datesubmitted` or `encounterdatefrom` within contract dates)
- `PA DATA` - Prior authorizations (filter by `requestdate` within contract dates)
- `PROVIDERS` - Hospital/provider information
- `TBPADIAGNOSIS` - Diagnoses for analysis
- `DIAGNOSIS` - Diagnosis descriptions
- `PROCEDURE DATA` - Procedure descriptions

**Contract Period Logic:**
- Get current contract: `SELECT * FROM GROUP_CONTRACT WHERE groupname = '[COMPANY_NAME]' AND iscurrent = 1`
- Get last contract: `SELECT * FROM GROUP_CONTRACT WHERE groupname = '[COMPANY_NAME]' AND iscurrent = 0 ORDER BY enddate DESC LIMIT 1`
- Use `startdate` and `enddate` from contracts to filter all financial and medical data

**MLR Calculation:**
- MLR = (Total Medical Spending / Total Premium Received) × 100%
- Total Medical Spending = Claims (`approvedamount`, usually filtered by `encounterdatefrom`) + Unclaimed PA (`granted` where PA panumber is not in the claims panumber set for the same scope/period)
- Total Premium Received = Sum of CLIENT_CASH_RECEIVED.assets_amount for the contract period

**Important Notes:**
- Always compare like-for-like periods (full contract periods, not partial)
- Consider seasonality if comparing different months
- Look for outliers (single large claims, new expensive providers)
- Consider member count changes between contracts

