# Lecture 10: Metadata in Data Warehousing

## I. Introduction and Importance of Metadata

Metadata is fundamentally defined as **data about data**. Its primary function is to provide the **background and context** that gives concrete meaning to the facts and figures stored in a data warehouse.

Metadata is often considered the weakest aspect of a data warehouse because it is frequently overlooked or under-implemented. However, without a robust metadata solution, data warehouse customers face risks because they may only see numbers without knowing their source, formula, completeness, or timeliness.

### The Need for Context

Metadata serves to mitigate the risks arising from assumptions made by designers or users regarding the meaning and context of the data.

| User Scenario                                                                                             | Problem without Metadata                                                                    | Metadata Solution                                                                                                                  |
| :-------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------- |
| **Fred, the Analyst:** Profitability report changed from 12.7% to 3.1% between January 15 and January 23. | Fred does not know _what_ changed or _why_ the numbers are different.                       | Metadata would show that fourth-quarter labor payroll adjustments were loaded on January 16, drastically increasing labor expense. |
| **Susan, the Auditor:** Needs to verify how performance ratios were calculated.                           | Susan needs a programmer to explain the ETL application and BI Reporting application logic. | Metadata would explicitly document the formulas, transformations, and ETL processes used to derive the ratios.                     |
| **Alice, the Sales Manager:** Observes a sudden downward trend in sales.                                  | Alice does not know if the drop is real or due to data availability issues.                 | Metadata would reveal that three franchise outlets failed to send their sales data to the warehouse.                               |

Metadata provides the context, definitions, and quality metrics that make data trustworthy, helping the organization realize the principle of "one version of the truth".

## II. Types of Metadata: Static vs. Dynamic (Ref. 2: Chapter 9)

Metadata is categorized primarily into two types based on what information they describe and how frequently they change.

### A. Static Metadata (Technical and Business Definitions)

**Static Metadata** is information that does not change across all instances of a data element. It remains constant regardless of the number of rows or the time of inquiry.

1.  **Purpose:** Static Metadata provides the fundamental **business meaning and origin** of a data element, enabling business users to correctly select and use the data.
2.  **Scope:** It includes descriptions for entities in Dimension tables, events in Fact tables, and derived fields in BI Reports.
3.  **State-Specific:** When a data element changes state (e.g., from "Reviewed" to "Approved"), the Static Metadata description is specific to each state.
4.  **Modeling Context:** In a Metadata Repository, Static Metadata is typically represented as **Dimensions**.
5.  **Examples:** The table of contents of a book, or the nutrition information on a food label, are analogous to static metadata as they describe the unchanging nature of the item.

### B. Dynamic Metadata (Operational and Temporal Context)

**Dynamic Metadata** describes **each individual instance** of a data element and tracks the events or actions that affect the data warehouse.

1.  **Purpose:** Dynamic Metadata documents the precise moments and processes involved in updating the data warehouse, which is crucial for a time-variant system.
2.  **Key Fields (Audit/Lineage):** A common form is the **Load Timestamp** field on each row, recording the precise moment of insertion.
3.  **Content:** Dynamic Metadata captures details about ETL jobs, including the job number, start time, end time, and duration for extraction, transformation, and loading steps. It also tracks the name, timestamp, and version of BI Reports that utilized the data.
4.  **Source:** Dynamic Metadata originates from multiple sources, including the ETL application platform, system clocks, system tables, and log files.
5.  **Modeling Context:** Dynamic Metadata is typically represented as **Facts** in a Metadata Repository, describing the update events.

### C. Static Metadata vs. Dynamic Metadata

| Type                 | Definition & Purpose                                                                                                                                               | Concrete Examples                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| :------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Static Metadata**  | Provides information about a data element that **does not change** across all instances. Its audience is typically the business side, defining meaning and origin. | **1. Data Dictionary:** The definition of a column in a table. <br> **2. Business Meaning:** The precise **definition of "active customer"** (e.g., "Customer who performed at least one transaction in the last 3 months"). <br> **3. Physical Description:** The attributes of a physical entity like a product, such as its name, data type, and currency used for monetary attributes.                                                                                                                                                  |
| **Dynamic Metadata** | Describes **each individual instance** of a data element and captures time-sensitive operational events (updates).                                                 | **1. Data Audit/Lineage:** A **Load Timestamp** field on a Fact table row indicating the subsecond moment that row was inserted. <br> **2. ETL Process Tracking:** The **job number, start time, end time, and duration** of the Extract, Transform, and Load jobs that processed the data. <br> **3. Dimensional Changes:** For Slowly Changing Dimension (SCD) Type 2 tracking, metadata columns record the **Start Date and End Date** of a particular version of a dimension member (e.g., tracking when a customer's address changed). |

## III. Business vs. Technical Metadata (Ref. 2: Chapter 9)

While Static and Dynamic categorize metadata by constancy, Business and Technical categories separate metadata by audience and purpose.

| Aspect           | Business Metadata (Semantics/Meaning)                                                                                                      | Technical Metadata (Structure/Process)                                                                                                                                                   |
| :--------------- | :----------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Definition**   | Describes the meaning, semantics, organizational rules, policies, and constraints related to the data.                                     | Describes how data is structured and stored in a computer system.                                                                                                                        |
| **Audience**     | Business users, managers, and Data Stewards.                                                                                               | Database administrators, ETL developers, system architects.                                                                                                                              |
| **Key Examples** | Definition of "active customer", retention policy (e.g., store data for 10 years after graduation), currency used for monetary attributes. | Table structure (e.g., name, data type, index), ETL transformation rules, data lineage (tracing warehouse data back to the source), security information (audit trails, access control). |

**Technical Metadata** is particularly extensive in a data warehouse context, covering the structure of the warehouse itself, the source systems, and the crucial ETL process.

## IV. Metadata Repository Architecture (Ref. 2: Chapter 9, Central & Distributed)

A **Metadata Repository** (often referred to conceptually as a Metadata Warehouse) is a structured database designed to manage metadata, storing Static Metadata as Dimensions and Dynamic Metadata as Facts.

### A. Central Metadata Repository

In a central model, the Metadata Repository acts as a single, specialized system.

1.  **Dimensional Model:** This highly effective design treats Static Metadata (Tables, Columns, Processes) as **Dimensions** and Dynamic Metadata (Inserts, Updates, Deletes, BI Report Executions) as **Facts**.
2.  **Batch Identifier (Batch_ID):** A surrogate key, such as **Batch_ID**, is used to link a group of data warehouse rows to their corresponding Dynamic Metadata (Fact) record. The Batch_ID must be stored in the most compressed format possible to reduce overhead, as it holds no inherent business meaning.
3.  **Third Normal Form (3NF):** While Dimensional models denormalize metadata for simple querying, a 3NF repository offers greater flexibility, for instance, by allowing a single ETL process to be associated with multiple transformations without requiring a separate row for each combination. Both Dimensional and 3NF repositories should utilize **Type II time-variant data models** to preserve historical context.

### B. Distributed Metadata Repository

A distributed repository contrasts with the centralized model by integrating metadata information directly into the data warehouse rows.

- **Mechanism:** Instead of requiring external join tables (as in the central model), the join between a data warehouse row and its Metadata Dimensions is **embedded within each data warehouse row**. This avoids duplicating every data warehouse table as metadata tables.
- **Best Use Case:** Row-level metadata is generally most useful when implemented within an **Operational Data Store (ODS)** where the data currency is high, focusing attention on immediate activity.

## V. Real-Time Metadata (Ref. 2: Chapter 9)

In environments employing Real-Time ETL applications, data is continuously loaded at maximum possible throughput. Capturing metadata for this continuous stream presents a time challenge.

1.  **Granularity Challenge:** A real-time ETL application might run an iteration every six seconds, leading to **14,400 individual iterations** of metadata to record in a single day.
2.  **Usability Constraint:** Data warehouse customers may not be able to effectively use or leverage metadata recorded at a sub-second granularity.
3.  **Solution (Batch Aggregation):** If real-time detail is unusable, the Metadata Repository will **superimpose a surrogate Batch_ID**. This Batch_ID identifies all data warehouse rows received during a specific time range (e.g., five seconds, five minutes, or one hour) or associated with a major systemic event (e.g., a Trade Cycle or Purchase Order). This ensures that customers can access data warehouse rows efficiently, rather than having to search for a "needle in a haystack".

## VI. Metadata Service Level Agreement (SLA) (Ref. 2: Chapter 9)

The **Metadata SLA** documents and formalizes the organizational requirements for metadata quality and availability. SLAs are vital for ensuring that data is available to users at all necessary times.

- **Documenting Requirements:** The SLA serves as an official agreement between the data warehouse team and Data Stewards regarding what Dynamic Metadata will be loaded into the repository.
- **Static Metadata Requirements:** The SLA dictates the essential features and layout of the data dictionary and establishes the methods by which customers can access and view that dictionary data.
- **Dynamic Metadata Guarantees:** By including a requirement in the Metadata SLA, the parties agree that the specific Dynamic Metadata is available, the ETL application will retrieve it, and the metadata will be made available to the business user or application that mandated it.

## VII. Make or Buy a Metadata Repository (Ref. 2: Chapter 9)

When implementing a Metadata Repository, organizations face the strategic decision of purchasing an off-the-shelf application or building a custom solution in-house.

| Option                                 | Evaluation                                                                                                                     | Advantages                                                                                                                                                                                                                                                                                                                   | Disadvantages / Considerations                                                                                                                                                   |
| :------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Buy (Off-the-shelf Application)**    | Reviewing vendor-supplied Metadata Repository applications is recommended, as they embody years of experience and development. | **Optimal Compromise:** Often the best solution when facing tight budgetary or timeline constraints. **Feature Richness:** Reveals valuable functions and features considered essential in the industry, which can inform the design if the organization chooses to build.                                                   | Cost, integration complexity, potential mismatch with highly unique business requirements. Delivering a purchased repository is still better than delivering none or a weak one. |
| **Make (In-house Custom Development)** | Designing a repository tailored specifically to the organization's unique data environment.                                    | **Custom Fit:** Ensures the repository perfectly addresses the specific integration, transformation, and business semantics of the organization. **Design Integrity:** An in-house solution benefits from established design standards, preventing future designers from reinterpreting the model and introducing confusion. | Requires significant time, resources, and expert staff to design and build a feature-complete solution from scratch.                                                             |
