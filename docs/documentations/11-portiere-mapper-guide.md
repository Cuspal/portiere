# Portiere Mapper Guide

Portiere Mapper is a gamified crowdsourcing platform for clinical concept mapping. Built as a LINE LIFF (LINE Front-end Framework) mini-app, it enables clinical informaticists, medical coders, pharmacists, and other domain experts to contribute to concept mapping efforts through a mobile-first voting interface with financial incentives.

---

## Table of Contents

1. [Overview](#overview)
2. [Intended Audience](#intended-audience)
3. [Onboarding Flow](#onboarding-flow)
4. [Approval Gates](#approval-gates)
5. [Voting Workflow](#voting-workflow)
6. [Leaderboard and Reputation](#leaderboard-and-reputation)
7. [Payout Mechanics](#payout-mechanics)
8. [Quality Control](#quality-control)
9. [Admin Dashboard](#admin-dashboard)

---

## Overview

Portiere Mapper addresses the bottleneck in clinical data harmonization: the need for expert human review of concept mappings. Rather than relying solely on in-house teams, Portiere Mapper distributes mapping review tasks to a vetted community of clinical domain experts.

### Key Features

- **LINE LIFF integration**: Accessible directly within the LINE messaging app, reducing friction for users in regions where LINE is prevalent.
- **Gamification**: Leaderboard rankings, reputation scores, and per-vote financial rewards incentivize participation and quality.
- **Quality assurance**: Consensus-based voting, expert review layers, and reputation-weighted scoring ensure mapping accuracy.
- **Stripe Connect payouts**: Verified mappers receive financial compensation for their contributions.

### Technical Stack

- **Frontend**: Next.js App Router with Tailwind CSS
- **Authentication**: LINE LIFF SDK (`liff.ts`)
- **Backend**: Supabase (database and real-time subscriptions)
- **Payments**: Stripe Connect (`payout.ts`)
- **Admin**: Role-based admin access controlled by LINE user IDs (`admin-auth.ts`)

---

## Intended Audience

Portiere Mapper is designed for professionals with clinical terminology expertise:

- **Clinical Informaticists**: Specialists in health information systems and clinical data standards (OMOP, FHIR, HL7).
- **Medical Coders**: Professionals experienced in ICD-10, CPT, SNOMED CT, and other medical classification systems.
- **Pharmacists**: Experts in drug terminologies (RxNorm, ATC, NDC) and pharmaceutical data mapping.
- **Clinical Data Managers**: Professionals managing clinical trial data who understand CDISC, MedDRA, and WHO-Drug standards.
- **Biomedical Researchers**: Researchers familiar with ontologies and controlled vocabularies in the biomedical domain.

Participation requires demonstrating domain expertise during the onboarding process.

---

## Onboarding Flow

New mappers must complete a multi-step onboarding process before they can begin voting on mapping tasks. The onboarding page is located at `/onboarding/page.tsx` and is handled by the `/api/onboard` API route.

### Step 1: LINE Login

1. Open the Portiere Mapper LIFF URL within the LINE app.
2. The LIFF SDK (`liff.ts`) handles authentication automatically.
3. Your LINE profile (display name, user ID, profile picture) is retrieved and used as your mapper identity.

### Step 2: Registration

After LINE login, complete the registration form:

- **Full legal name**: Required for identity verification and payout processing.
- **Email address**: For notifications and account recovery.
- **Professional title**: Your current role or position.
- **Affiliation**: Your organization or institution.
- **Areas of expertise**: Select the clinical domains and vocabularies you are proficient in (e.g., ICD-10, SNOMED CT, RxNorm, LOINC).
- **Years of experience**: Your years of relevant professional experience.

### Step 3: KYC Document Upload

Upload identity verification documents:

- **Government-issued ID**: Passport, national ID card, or driver's license.
- **Professional credential**: Medical license, certification, or institutional badge.

Documents are securely stored in Supabase Storage and reviewed by administrators.

### Step 4: Bank Information for Payouts

Provide banking details for receiving payout via Stripe Connect:

- **Bank account details**: Account number and routing information.
- **Tax identification**: Required for compliance in certain jurisdictions.

Stripe Connect handles the secure storage and processing of financial information. Portiere does not store raw banking credentials.

### Step 5: Submit for Review

After completing all steps, your application is submitted for administrator review. You will be redirected to the `/pending/page.tsx` page while your application is processed.

---

## Approval Gates

The `ApprovalGate` component (`ApprovalGate.tsx`) controls access to the mapper platform based on the applicant's current status. The approval flow has three states:

### Pending

- **Page**: `/pending/page.tsx`
- **Description**: Your application has been submitted and is awaiting administrator review.
- **What you see**: A status page indicating your application is under review, with an estimated processing time.
- **What happens next**: An administrator reviews your credentials, KYC documents, and professional background.

### Approved

- **Description**: Your application has been approved. You have full access to the voting interface, leaderboard, and profile pages.
- **What you can do**: Vote on mapping tasks, earn rewards, and appear on the leaderboard.

### Rejected

- **Page**: `/rejected/page.tsx`
- **Description**: Your application has been rejected by an administrator.
- **What you see**: A page explaining the rejection with an optional reason provided by the administrator.
- **Options**: You may be able to reapply after addressing the stated concerns, depending on the rejection reason.

---

## Voting Workflow

The voting interface is the core of Portiere Mapper. Approved mappers receive mapping tasks and provide expert judgment on suggested concept mappings.

### Receiving a Task

1. Navigate to the home page (`/page.tsx`) after login.
2. Available mapping tasks are presented based on your declared areas of expertise.
3. Select a task to begin reviewing.

### Reviewing a Mapping Task

The voting page (`/vote/[taskId]/page.tsx`) presents:

- **Source code**: The original clinical code from the source dataset, including its code system and description.
- **Source context**: Additional metadata such as the source table, column, and any available context (e.g., surrounding data fields).
- **Candidate mappings**: A ranked list of suggested target concept matches, each showing:
  - Target concept code and description
  - Concept vocabulary (SNOMED CT, ICD-10, RxNorm, etc.)
  - Confidence score from the automated pipeline
  - Semantic similarity details

### Casting a Vote

The voting library (`voting.ts`) supports three actions:

1. **Approve**: Accept the top-ranked candidate as the correct mapping. Use this when the suggested mapping is accurate and complete.

2. **Reject**: Reject the top-ranked candidate. This indicates that the suggested mapping is incorrect. You may optionally select a different candidate from the list.

3. **Override**: Provide a different mapping entirely. Use the concept search functionality to find the correct target concept that is not among the candidates.

Each vote is recorded via the `/api/vote` API route and associated with your mapper profile for reputation tracking.

### Consensus

A mapping task is resolved when sufficient votes have been collected. The consensus mechanism considers:

- **Vote count**: A minimum number of independent votes is required.
- **Agreement threshold**: A configurable percentage of votes must agree on the same mapping.
- **Reputation weighting**: Votes from higher-reputation mappers carry more weight.

---

## Leaderboard and Reputation

The leaderboard page (`/leaderboard/page.tsx`) displays mapper rankings and encourages quality participation.

### Reputation Score

Your reputation score is calculated based on:

- **Accuracy**: How often your votes align with the final consensus.
- **Consistency**: Your track record over time (recent accuracy weighted more heavily).
- **Volume**: Total number of tasks completed.
- **Expertise alignment**: Performance in your declared areas of expertise versus other domains.

### Leaderboard Rankings

The leaderboard shows:

- **Rank**: Your position relative to other mappers.
- **Display name**: Your LINE profile display name.
- **Total votes**: Number of mapping tasks you have voted on.
- **Accuracy rate**: Percentage of your votes matching the final consensus.
- **Reputation score**: Your composite reputation metric.
- **Earnings**: Total payout earned (optionally displayed).

### Ranking Tiers

Mappers are grouped into tiers based on their reputation score. Higher tiers may unlock:

- Priority access to high-value mapping tasks.
- Increased per-vote payout rates.
- Access to specialized or complex mapping tasks.

---

## Payout Mechanics

Portiere Mapper compensates approved mappers for their contributions using Stripe Connect, managed through the `payout.ts` library and the `/api/payout` API route.

### Per-Vote Rewards

- Each completed vote earns a base reward amount.
- The reward amount may vary based on:
  - **Task complexity**: More complex mappings (lower automated confidence) pay more.
  - **Mapper tier**: Higher-tier mappers may earn higher per-vote rates.
  - **Consensus bonus**: Additional reward if your vote matches the final consensus.

### Payout Processing

1. **Accrual**: Rewards accrue in your mapper account as you complete votes.
2. **Batch processing**: Payouts are processed in batches by administrators.
3. **Stripe Connect transfer**: Funds are transferred to your connected bank account via Stripe Connect.
4. **Confirmation**: You receive a notification (via LINE) when a payout is processed.

### Payout Requirements

- Your Stripe Connect account must be fully verified.
- A minimum payout threshold must be reached before a transfer is initiated.
- Payouts are subject to applicable tax withholding requirements.

### Viewing Payout History

Navigate to your profile page (`/profile/page.tsx`) to view:

- Current accrued balance.
- Payout history with dates, amounts, and statuses.
- Stripe Connect account status and verification state.

---

## Quality Control

Portiere Mapper employs multiple layers of quality control to ensure mapping accuracy.

### Consensus Thresholds

- A configurable minimum number of votes per task ensures no single mapper's judgment determines the outcome.
- Agreement thresholds (e.g., 70% of voters must agree) prevent ambiguous mappings from being accepted.

### Expert Review

- Mappings that fail to reach consensus are escalated to expert reviewers.
- Expert reviewers are high-reputation mappers or designated administrators with specialized domain knowledge.
- Expert votes carry elevated weight in the consensus calculation.

### Reputation-Weighted Voting

- Votes are weighted by the mapper's reputation score.
- New mappers with unestablished reputation have lower vote weights.
- Reputation-weighted consensus reduces the impact of low-quality votes.

### Spot Checks and Calibration Tasks

- Periodically, mappers receive calibration tasks with known correct answers.
- Performance on calibration tasks influences reputation scores.
- Consistently poor performance on calibration tasks may trigger a review of mapper status.

### Anomaly Detection

- Voting patterns are monitored for anomalies (e.g., unusually fast voting, systematic patterns).
- Suspicious activity triggers a review by administrators.

---

## Admin Dashboard

The admin dashboard provides tools for managing the mapper community, reviewing applications, and processing payouts. Access is restricted to users whose LINE user IDs are listed in the `ADMIN_LINE_USER_IDS` environment variable, enforced by `admin-auth.ts`.

### Accessing the Admin Dashboard

1. Navigate to `/admin/page.tsx`.
2. Admin authentication is verified via your LINE user ID against the configured `ADMIN_LINE_USER_IDS`.
3. Non-admin users are denied access.

### Reviewing Applications

The application review page (`/admin/review/[mapperId]/page.tsx`) provides:

- **Applicant profile**: Name, affiliation, professional title, areas of expertise.
- **KYC documents**: Uploaded identity and credential documents for verification.
- **Bank information**: Stripe Connect account status.

Actions available through the `/api/admin/applications` API route:

1. **Approve**: Grant the applicant mapper status, allowing them to vote on tasks.
2. **Reject**: Deny the application with an optional reason. The applicant is notified and redirected to `/rejected/page.tsx`.
3. **Request more information**: Ask the applicant to provide additional documentation.

### Managing Mappers

From the admin dashboard, administrators can:

- View all registered mappers with their status, reputation, and activity metrics.
- Suspend or deactivate mappers who violate quality standards.
- Adjust mapper tiers and payout rates.

### Processing Payouts

1. Navigate to the payouts section of the admin dashboard.
2. Review pending payouts with mapper details, accrued amounts, and vote counts.
3. Select payouts to process (individually or in batch).
4. Confirm the payout batch. Stripe Connect handles the fund transfers.
5. Monitor payout status (pending, processing, completed, failed).

### Environment Variables

The following environment variables must be configured for the Portiere Mapper admin system:

| Variable                        | Description                                                  |
|---------------------------------|--------------------------------------------------------------|
| `NEXT_PUBLIC_LIFF_ID`           | LINE LIFF application ID                                     |
| `LINE_CHANNEL_ID`               | LINE channel ID for server-side verification                 |
| `NEXT_PUBLIC_SUPABASE_URL`      | Supabase project URL                                         |
| `SUPABASE_SERVICE_ROLE_KEY`     | Supabase service role key (server-side only)                 |
| `ADMIN_LINE_USER_IDS`           | Comma-separated list of LINE user IDs with admin access      |
| `STRIPE_SECRET_KEY`             | Stripe secret key for payout processing                      |

---

## Related Documentation

- [Portiere Cloud Guide](10-portiere-cloud-guide.md) -- Cloud dashboard for project management
- [Deployment Guide](12-deployment.md) -- Deploy the Mapper platform
- [Quickstart Guide](01-quickstart.md) -- Get started with the Portiere SDK
