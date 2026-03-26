# Pereira Corruption Detector - Dashboard SPEC

## Goal
Build a professional investigative dashboard for journalists, auditors, and investigators to explore, filter, and analyze corruption data from SECOP contracts.

## Users
- **Journalists** — Need quick insights, exportable findings, story leads
- **Auditors** — Need detailed filtering, audit trails, bulk data
- **Investigators** — Need network visualization, deep-dive on entities, cross-reference

## Scope

### In Scope
- Interactive data table with filtering & sorting
- Search by contractor, contract ID, keywords, officials
- Filter by: date range, contract value, suspicion score, contract type, municipality
- Contract detail view with suspicion explanations
- Entity lookup (company/official profiles with history)
- Export to CSV/Excel
- Dashboard summary statistics

### Out of Scope
- User authentication (future)
- Real-time data refresh (manual refresh only)
- Gephi network visualization (keep in Streamlit for now)

---

## Functionality

### 1. Dashboard Overview
- **Stats cards**: Total contracts, flagged contracts, unique vendors, total value
- **Suspicion distribution**: Histogram of suspicion scores
- **Trend over time**: Contracts flagged per month/year
- **Top risk categories**: Bar chart of common risk factors

### 2. Contract Explorer (Main View)
- **Data table** (virtual scrolling for performance):
  - Columns: ID, Title, Vendor, Contract Type, Value, Date, Suspicion Score, Risk Flags
  - Sortable by any column
  - Resizable columns
- **Filters sidebar**:
  - Date range (from → to)
  - Contract value range (min → max)
  - Suspicion score (Low/Medium/High/Critical)
  - Contract type (multi-select)
  - Municipality
  - Risk factors (single bidder, new vendor, etc.)
- **Search bar**: Full-text search across title, description, vendor name

### 3. Contract Detail View
- **Header**: Contract ID, Title, Status, Value
- **Parties**: Vendor info, awarding entity, officials involved
- **Suspicion breakdown**: Why this contract was flagged (with severity levels)
- **Timeline**: Key dates (published, awarded, start, end)
- **Related contracts**: Other contracts with same vendor/official

### 4. Entity Profiles
- **Vendor profile**: Company name, RUES info, registration date, contract history, suspicion history
- **Official profile**: Name, role, department, contracts awarded, network connections

### 5. Export
- Export current filtered view to CSV
- Export selected contracts to CSV
- Export full dataset option

---

## Acceptance Criteria

1. ✅ Dashboard loads in <3 seconds with 10k contracts
2. ✅ All filters apply instantly (no page reload)
3. ✅ Search returns results in <500ms
4. ✅ Suspicion explanations are clear and actionable
5. ✅ Export produces valid CSV with all filtered columns
6. ✅ Mobile-responsive (usable on tablet)
7. ✅ Entity lookup returns historical contracts for that vendor/official
8. ✅ No exposed credentials or API keys in UI