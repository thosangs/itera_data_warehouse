# Lecture 9: The ETL Process in Data Warehousing

## I. ETL Process: ETL vs. ELT Architecture (Ref. 2: Chapter 6, Architecture)

The set of applications that populate a data warehouse is referred to as Data Acquisition and Integration, commonly known as **Extract, Transform, and Load (ETL)**. The ETL application serves as the most comprehensive line between the source systems (the enterprise) and the target system (the data warehouse).

### A. Extract, Transform, and Load (ETL)

ETL is a three-step process designed to feed data from internal and external sources into the data warehouse.

1.  **Extract (E) / Data Acquisition:** This function reaches into a source system to retrieve data, yielding what is known as _Source Data_. Extraction gathers data from multiple, heterogeneous sources, which may include files in various formats, operational databases, or external systems.
2.  **Transform (T) / Data Integration (First Half):** This function inspects, cleanses, and conforms the Source Data to the data warehouse requirements, resulting in _Load Data_. Transformations involve cleaning data (removing errors and inconsistencies), integrating data (reconciling schemas and data from different sources), and aggregating data (summarizing data based on the required granularity).
3.  **Load (L) / Data Integration (Second Half):** This function updates the data warehouse using the Load Data. The loading process propagates updates from source systems to keep the data warehouse refreshed at a specified frequency.

#### Traditional Architecture (ETL)

Early ETL applications were physically designed assuming three separate platforms: the source system platform, the ETL platform, and the Data Warehouse platform. In this model, the transformation occurs on the dedicated ETL platform _before_ the data is loaded into the final DW tables.

### B. Extract, Load, and Transform (ELT)

The ELT architecture emerged when ETL analysts began leveraging the computational power of the target RDBMS (the data warehouse itself). The ELT paradigm is the inverse approach to ETL.

1.  **Process Flow:**
    - **Extract:** Operational data is extracted.
    - **Load:** The extracted data is loaded directly into staging tables located _on the data warehouse RDBMS platform_.
    - **Transform:** All transformation functions are then performed inside the powerful data warehouse RDBMS platform.
2.  **Modern Relevance:** The ELT paradigm is central to modern concepts like data lakes. It is highly effective in cloud data warehouse architectures (e.g., BigQuery, Snowflake) due to their massive storage and computing resources, enabling transformations to occur directly within the environment.
3.  **Key Differences Summary:**
    | Feature | ETL | ELT |
    | :--- | :--- | :--- |
    | Transformation Location | Dedicated ETL Platform (or Staging Area) | Target Data Warehouse RDBMS Platform |
    | Sequence | Extract $\to$ Transform $\to$ Load | Extract $\to$ Load $\to$ Transform |
    | Compute Utilization | Dedicated ETL Server/Platform | Data Warehouse Resources |
    | Data Stored in DW | Clean, transformed data | Raw data is staged, transformed data is finalized |

## II. ETL Design Principles (Ref. 2: Chapter 6, ETL Process Principles)

ETL Design Principles are lessons learned intended to ensure that ETL applications are **robust** and **reliable** (bulletproof), regardless of unexpected circumstances or variations in source data. These six principles provide methods for managing and controlling data as it passes through the ETL process.

| Principle                    | Concept                              | Detail                                                                                                                                                                                                                                                            |
| :--------------------------- | :----------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **One Thing at a Time**      | Granular modularity.                 | ETL applications should avoid multitasking within a single piece of code. This separation isolates individual functions, allowing easier detection of violated assumptions and enabling integrated Data Quality and Metadata functions.                           |
| **Know When to Begin**       | Data-driven prerequisite check.      | An application starts based on specified _Begin Conditions_ existing within the precedent data itself, rather than solely relying on job scheduler completion codes. An Extract application should examine the source data to confirm it is ready for extraction. |
| **Know When to End**         | Forward-looking output verification. | An ETL application examines the data it has created (_End Conditions_) to verify that its output satisfactorily meets expectations before releasing it to subsequent applications or customers.                                                                   |
| **Large to Medium to Small** | Data volume management.              | The ETL process should begin with the largest possible set of applicable data and progressively reduce it (Medium to Small) during transformation. This ensures that decisions to exclude data are made in the broadest possible context for maximum control.     |
| **Stage Data Integrity**     | Maintaining dataset unity.           | Once created, a set of staged data must be consumed as a single, contiguous set. Data intended for consumption by later applications should be stored physically as a single set to avoid introducing unnecessary extract functions later in the pipeline.        |
| **Know What You Have**       | Data Inventory.                      | ETL applications should actively take an inventory of inbound data and compare its contents with expected data, avoiding the assumption that incoming data is complete.                                                                                           |

## III. Source System Analysis (Ref. 2: Chapter 3)

Source System Analysis (SSA) is a foundational stage in data warehousing, acting as an analysis of the enterprise through its data. The outcome of SSA forms the necessary context for effective ETL application design.

### A. Core Principles for Source System Analysis

SSA principles identify the critical information a data warehouse designer must gather about the enterprise's data:

- **System of Record (SOR):** Identifying the one dataset or application recognized as the authoritative and original expression (point of origin) of a specific data element, especially when data is duplicated across business units.
- **Entity Data:** Data that describes and qualifies the physical (e.g., facilities, agents) and logical (e.g., members) components of the enterprise.
- **Arithmetic Data:** Quantitative measurements of business activity.
- **Transaction Data:** Data that captures a single event using a conjunction of entity and arithmetic data.
- **Snapshot Data:** Data measuring the net effect of multiple events over time, which is less granular than Transaction Data.
- **Granularity and Latency:** Understanding the hierarchical level of detail (granularity) and the delay until data availability (latency).

### B. Methods of Source System Analysis

SSA uses methods to understand not only the content of the data but also its movement and validity:

1.  **Data Profile:** Provides a static view of the enterprise's data, documenting inventories of data elements and including logical and physical data models of the source system.
2.  **Data Flow Diagram:** Provides a dynamic view of data in motion. It traces where data originates, where it goes, and the transport mechanisms used, adding dimensions of time, sequence, and movement.
3.  **Data State Diagram:** Provides a dynamic view relating data to its business relevance and meaning. This is the opportunity to document the business rules governing data in the source system.
4.  **System of Record Identification:** The process of discerning which data source is the authoritative one, answering the fundamental ETL design question: "Where do I get the enterprise data?".

## IV. BPMN for ETL (Ref. 1: Chapter 9.1-9.2)

The complexity and cost associated with ETL processes necessitate careful conceptual modeling. **Business Process Modeling Notation (BPMN)** is a _de facto_ standard appropriate for modeling ETL processes because ETL tasks are viewed as workflows similar to business processes.

### A. Conceptual ETL Design using BPMN

BPMN provides a conceptual and implementation-independent specification, which is advantageous for deploying the model across different ETL tools.

1.  **ETL Tasks as Control and Data Flows:**
    - **Control Tasks:** Represent the orchestration and sequence of ETL tasks (workflows), controlling the flow using gateways (e.g., parallel or exclusive) and managing process flow using events (e.g., a cancelation event for error handling).
    - **Data Tasks:** Detail the input, transformation, and output of data, where arrows represent the actual data flow/transfer of records.
2.  **BPMN Elements for ETL Modeling:**
    - **Flow Objects (Activities, Gateways, Events):** Define the sequence and actions.
    - **Swimlanes:** Used to organize ETL processes, potentially by technical architecture (e.g., Server 1, Server 2) or business entities.
    - **Artifacts (Annotations):** These are critical for conceptual ETL design, adding metadata to tasks to specify parameters or semantics. For instance, a **Lookup** task annotation defines the database, table, retrieval key, and matching criteria. Other data tasks include **Input Data**, **Insert Data**, **Add Columns**, **Convert Columns**, and **Rename Columns**.

## V. Changed Data Capture (CDC) (Ref. 2: Chapter 6)

Changed Data Capture (CDC) is a vital ETL function designed to efficiently track and capture only the data changes in the source system, thereby supporting incremental loads and ensuring data synchronization.

### A. Mechanism and Functions

- **Change Detection:** A key requirement of ETL applications is identifying and capturing Dimension updates. This is typically achieved by juxtaposing Dimension data in the DW against corresponding Dimension data in the source system.
- **SQL Server CDC:** SQL Server implements CDC as a built-in audit-tracking system that automatically records change activity: **INSERT**, **UPDATE**, and **DELETE**.
- **Operation Details:** CDC tracks inserted rows, deleted rows (recording column values before deletion), and updated rows (recording values both before and after the update).
- **Asynchronous Processing:** CDC operates asynchronously by reading the transaction log file, ensuring minimal performance impact and no contention with ongoing source transactions.

### B. CDC Approaches

CDC primarily occurs in two forms: Universe to Universe and Candidate to Universe.

1.  **Universe to Universe CDC (Best Practice):**
    - **Method:** Compares the _entire_ universe of data values at moment #1 against the entire universe of values at moment #2.
    - **Benefit:** Captures the true net effect of all changes and ensures no updates escape notice.
2.  **Candidate to Universe CDC:**
    - **Method:** Compares individual rows retrieved from the source system (Candidate rows) against the universe of data in the data warehouse.
    - **Use Cases:** Used when data volumes are too large for Universe to Universe comparison, or when real-time data delivery is required.
    - **Limitation:** This method cannot detect when a source system row is **terminated** (deleted) because a deleted row cannot be selected as a Candidate row for comparison.

## VI. ETL Staging (Ref. 2: Chapter 6, ETL Staging Principles)

A **data staging area** is a storage area where extracted data undergoes successive modifications and preparation before being loaded into the data warehouse. The ETL staging environment acts as a factory for data transformation.

ETL Staging Principles focus on controlling the physical manipulation and lifespan of interim data, enhancing the integrity and control of the overall ETL application.

| Principle          | Objective                                                                                                                                                                                                                                                      |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Name the Data**  | To achieve granular control, data must be uniquely identified at an appropriate level of detail (e.g., assigning lot numbers or VINs to datasets).                                                                                                             |
| **Own the Data**   | ETL applications must create their own copies (exclusive datasets) of source data to **freeze time** at a specific moment. This isolation prevents operational updates from interfering and is necessary for handling time-variant data.                       |
| **Build the Data** | Datasets must be defined and controlled **from the outside** (defining structure, layout, properties) before data is applied to them, rather than inheriting properties from existing data constraints.                                                        |
| **Type the Data**  | To protect against abnormal termination, the ETL application must verify that the data type of inbound or transformed data is compatible with the destination data type before the movement occurs (preventing mismatches, overflows, or null violations).     |
| **Land the Data**  | Interim data should be retained (landed) in cataloged datasets, rather than being temporary. Retained interim data is essential for problem investigation and subsequent data quality triage, as it remains available after the immediate processing job ends. |
