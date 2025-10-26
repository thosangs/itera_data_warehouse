This lecture material provides a detailed examination of Online Analytical Processing (OLAP), covering its core operations, fundamental architectural approaches, specialized query languages, and powerful analytical functions used in data warehousing environments.

---

# Lecture 11: Data Processing with OLAP

## I. Introduction to OLAP and Cube Fundamentals

**Online Analytical Processing (OLAP)** is a paradigm specifically designed for analytical queries that supports interactive data analysis for the decision-making process. It contrasts sharply with Online Transaction Processing (OLTP), which focuses on high-frequency transactions and operational efficiency.

### A. The Multidimensional Model (Data Cube)

OLAP systems are founded on the **Multidimensional Model**.

1.  **Data Cube:** Data is viewed in an n-dimensional space, referred to as a **data cube** or hypercube.
2.  **Dimensions:** These are the descriptive perspectives or viewpoints used to analyze the data (e.g., Time, Product, Location, Customer). Dimensions typically contain hierarchical structures (e.g., Year $\to$ Quarter $\to$ Month).
3.  **Measures/Facts:** These are the numerical or quantitative values being analyzed (e.g., Sales Revenue, Quantity Sold, Profit). Each cell in the cube represents the measured value for a specific intersection of dimension values.

## II. OLAP Operations (Ref. 1: Chapter 3.2)

The fundamental characteristic of the multidimensional model is that it enables viewing data from multiple perspectives and at various levels of detail through a set of interactive **OLAP operations**.

| Operation          | Concept                                                                                                                         | Purpose & Example                                                                                                                                                                |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Roll-up**        | Aggregates data by moving up one or more levels in a hierarchy.                                                                 | **Purpose:** To view summarized data or obtain coarser granularity. **Example:** Aggregating sales from the City level to the Country level or from daily sales to annual sales. |
| **Drill-down**     | Disaggregates measures by moving from a more general level to a more detailed level in a hierarchy.                             | **Purpose:** To reveal more granular detail. **Example:** Analyzing sales for a specific quarter by drilling down to monthly sales.                                              |
| **Slice**          | Removes a dimension by selecting and fixing a single instance (value) in a dimension level.                                     | **Purpose:** To create an $n-1$ dimensional subcube. **Example:** Filtering a sales cube to show data only for "Paris".                                                          |
| **Dice**           | Selects a subset of data based on a Boolean condition involving two or more dimension values.                                   | **Purpose:** To focus on a specific segment of the cube. **Example:** Viewing sales only for 'Paris' OR 'Lyon' AND Quarter 'Q1' OR 'Q2'.                                         |
| **Pivot (Rotate)** | Rotates the axes of the cube to provide an alternative, rotated presentation of the data.                                       | **Purpose:** To reorganize dimensions to change the primary focus of analysis. **Example:** Rotating a cube to switch Time from the row axis to the column axis.                 |
| **Drill-through**  | Allows moving from the bottom level data in the cube to the original, detailed source data records in the operational database. | **Note:** Although integral to analysis, it is formally not an OLAP operator as its result is not a cube.                                                                        |

## III. OLAP Architectures: MOLAP, ROLAP, and HOLAP (Ref. 2: Chapter 7)

OLAP applications offer three architectural approaches based on how the data cube structure and aggregations are stored. These models affect the tradeoff between storage capacity, performance, and complexity.

### A. Multidimensional OLAP (MOLAP)

MOLAP systems store both detailed data and precalculated aggregates in **specialized multidimensional data structures**, typically arrays (the cube itself).

- **Performance:** Provides the **fastest performance** because results are precalculated and stored in the cube, allowing near-instant retrieval. Queries are naturally efficient for typical OLAP functions.
- **Storage:** Requires **significant storage capacity** and high computation cycles for creation, as it stores every permutation of dimension intersection and a copy of the source data.
- **Scalability & Flexibility:** Often involves proprietary structures, limiting portability. Data reflects the state only as of the last time the cube was processed.

### B. Relational OLAP (ROLAP)

ROLAP systems leverage the underlying **relational database management system (RDBMS)**, storing multidimensional data and aggregates within relational tables (usually a Star or Snowflake schema).

- **Query Execution:** When a user queries data, the ROLAP cube generates a **complex SQL query** at runtime, which is submitted to the data warehouse for calculation.
- **Storage:** Requires the **least storage capacity** on the OLAP server because it does not maintain redundant copies of detailed data. It can handle large datasets efficiently.
- **Performance:** Generally provides the **slowest performance** compared to MOLAP, as complex aggregations are calculated dynamically at query time. However, it allows users to view data in real-time if accessed directly from the operational database.

### C. Hybrid OLAP (HOLAP)

HOLAP systems combine the strengths of both MOLAP and ROLAP.

- **Architecture:** **Aggregations** (summary data) are stored in the fast, multidimensional structure (MOLAP component), while **detailed, unaggregated data** is kept in the underlying relational database (ROLAP component).
- **Performance:** Achieves a compromise between performance and capacity. It is highly efficient for queries involving summary data (MOLAP speed) but must access the slower relational database when deep, unaggregated detail is required (ROLAP speed).
- **Storage:** HOLAP partitions are smaller than MOLAP partitions because they avoid storing redundant copies of the source data.

## IV. Cube, Rollup, and Aggregate Functions (Ref. 3: Chapter 19.3 & 19.2)

In relational databases (ROLAP implementation), Online Analytical Processing functionalities are often achieved through extensions to the SQL standard, known as SQL/OLAP.

### A. Basic Aggregate Functions (Ref. 3: Chapter 19.2)

Basic aggregate functions are frequently used in OLAP queries, often coupled with the $\text{GROUP BY}$ clause. These include $\text{SUM}$, $\text{AVG}$, $\text{COUNT}$, $\text{MAX}$, and $\text{MIN}$.

| Classification         | Functions                    | Description                                                             |
| :--------------------- | :--------------------------- | :---------------------------------------------------------------------- |
| **Cumulative**         | $\text{SUM, COUNT, AVG}$     | Compute the measure value of a cell by aggregating several other cells. |
| **Filtering**          | $\text{MIN, MAX}$            | Filter the members of a dimension that appear in the result set.        |
| **Extended Functions** | $\text{MEDIAN, VAR, STDDEV}$ | Specialized statistical functions often provided by OLAP tools.         |

### B. Cube and Rollup Extensions (Ref. 3: Chapter 19.3)

Since calculating all possible aggregations using traditional $\text{GROUP BY}$ statements requires $2^n$ queries for $n$ dimensions, SQL provides shorthands for aggregation computation:

1.  **GROUP BY CUBE:** An extension to the $\text{GROUP BY}$ clause that generates information in a **cross-tabulation format**. $\text{CUBE}$ computes **all possible subtotals** (aggregations) of the specified list of attributes, in addition to the grand total. This is useful when you need cross-totals in a matrix view.
2.  **GROUP BY ROLLUP:** Computes group subtotals based only on the **prefixes** of the attribute list specified. The $\text{ROLLUP}$ results are a subset of those generated by $\text{CUBE}$ because they exclude cross-combinations that are not prefixes in the hierarchy specified. This is useful when data has a natural hierarchy (e.g., Year $\to$ Month $\to$ Day).

## V. Ranking Functions (Ref. 3: Chapter 19.4)

Ranking is a critical OLAP operation commonly used for managerial decision-making, such as identifying top-performing products or locations. This functionality is implemented using **analytic functions** (or window functions) in SQL.

### A. Core Ranking Functions

| Function                        | Purpose                                                                | Tie Handling                                                                                           |
| :------------------------------ | :--------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------- |
| **$\text{RANK() OVER}$**        | Computes the rank of a record based on an ordering expression.         | Assigns the same rank to tied records and **skips** the subsequent rank.                               |
| **$\text{DENSE\_RANK() OVER}$** | Computes the rank of a record.                                         | Assigns the same rank to tied records but **does not skip** the subsequent rank, incrementing by 1.    |
| **$\text{ROW\_NUMBER() OVER}$** | Returns the sequential ordinal number of the row within its partition. | Assigns a **distinct value** to every row, even in the case of ties (not strictly a ranking function). |

### B. Advanced Ranking Clauses

1.  **$\text{PARTITION BY}$:** This clause is used within the $\text{RANK() OVER}$ function to divide the dataset into multiple independent partitions. Each partition then receives its own internal ranking, starting from rank 1. This is useful for answering questions like: "What is the best-selling product _within each region_?".
2.  **Top-N Ranking:** Focuses on retrieving the top $N$ ranks only. This is typically achieved by using a nested query where the inner query computes the full ranking using $\text{RANK() OVER}$, and the outer query filters for `Product_Rank <= N`.
3.  **$\text{PERCENT\_RANK() OVER}$:** Calculates the rank as a percentage or fraction, ranging from 0 (lowest) to 1 (highest). This is used for Top-Percentage Ranking (e.g., retrieving the top 75% of performers).

## VI. Multidimensional Query Languages: MDX and DAX

Since OLAP systems are highly specialized, two dominant languages—MDX and DAX—are used to query and manipulate the multidimensional structures.

### A. MDX (MultiDimensional eXpressions) (Ref. 1: Chapter 6.1)

MDX is the _de facto_ standard query language for querying **multidimensional databases** (cubes). It is widely supported by OLAP vendors.

1.  **Model:** MDX operates directly over **cubes, dimensions, hierarchies, and members**.
2.  **Core Syntax:** The primary query format is $\text{SELECT } \langle \text{axis specification} \rangle \text{ FROM } \langle \text{cube} \rangle \text{[WHERE } \langle \text{slicer specification} \rangle \text{]}$.
3.  **Axis Specification:** MDX can define up to 128 axes, although most tools only display two (COLUMNS and ROWS). The $\text{CROSSJOIN}$ function or the ‘\*’ operator is used to combine multiple dimensions onto a single axis for two-dimensional display.
4.  **Slicing (WHERE Clause):** The $\text{WHERE}$ clause in MDX specifies a **slice**, meaning it fixes a member from a dimension, effectively reducing the dimensionality of the resulting cube.
5.  **Functions:** MDX supports functions for complex time series analysis (e.g., $\text{PREVMEMBER}$, $\text{LAG}$ for calculating moving averages), and $\text{TOPCOUNT}$ for ranking/Top-N analysis.
6.  **Aggregation:** Aggregation functions ($\text{SUM, AVG}$) are generally defined within the cube definition itself and are automatically performed upon roll-up operations.

### B. DAX (Data Analysis eXpressions) (Ref. 1: Chapter 6.2)

DAX was introduced by Microsoft as an alternative to MDX, primarily for querying the **tabular model** (e.g., within Power BI or SQL Server Analysis Services Tabular mode). It is designed to be simpler and more accessible to business end-users.

1.  **Model:** DAX uses a simple model of tables joined implicitly through relationships, much like Excel functions.
2.  **Query Structure:** Unlike MDX and SQL, DAX does not use a $\text{SELECT}$ clause. It relies on functions like $\text{EVALUATE}$ and $\text{SUMMARIZECOLUMNS}$ to define and aggregate output.
3.  **Analysis Functions:**
    - **Ranking:** Uses the $\text{RANKX}$ function to rank values within a set.
    - **Top-N:** Uses the $\text{TOPN}$ function to display the $n$ topmost elements.
    - **Aggregation:** Measures are explicitly defined using the $\text{MEASURE}$ statement (e.g., `MEASURE Sales[Sales Amount] = SUM([SalesAmount])`).
4.  **Hierarchy Handling:** Handling complex or parent-child hierarchies is generally more challenging in DAX compared to MDX due to limited dedicated navigational functions.
