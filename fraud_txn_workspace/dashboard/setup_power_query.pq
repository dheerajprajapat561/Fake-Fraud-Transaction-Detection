// Power Query Script for Fraud Detection Dashboard
// Save this as a .pq file and import into Power BI Desktop

let
    // Load processed data
    Source = Csv.Document(File.Contents("C:\path\to\fraud_txn_workspace\data\processed_bank_data.csv"),[Delimiter=",", Columns=65, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{
        {"TransactionID", type text},
        {"TransactionDate", type datetime},
        {"TransactionAmount", type number},
        {"TransactionType", type text},
        // Add other columns as needed
        {"CustomerAge", Int64.Type},
        {"AccountBalance", type number}
    }),
    
    // Load predictions
    PredSource = Csv.Document(File.Contents("C:\path\to\fraud_txn_workspace\data\predicted.csv"),[Delimiter=",", Columns=10, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Pred Headers" = Table.PromoteHeaders(PredSource, [PromoteAllScalars=true]),
    #"Pred Changed Type" = Table.TransformColumnTypes(#"Pred Headers",{
        {"transaction_id", type text},
        {"fraud_probability", type number},
        {"is_fraud", Int64.Type}
    }),
    
    // Merge tables
    Merged = Table.NestedJoin(
        #"Changed Type",
        {"TransactionID"},
        #"Pred Changed Type",
        {"transaction_id"},
        "Predictions",
        JoinKind.LeftOuter
    ),
    #"Expanded Predictions" = Table.ExpandTableColumn(
        Merged,
        "Predictions",
        {"fraud_probability", "is_fraud"},
        {"Fraud_Probability", "Is_Fraud"}
    ),
    
    // Add calculated columns
    #"Added Custom" = Table.AddColumn(
        #"Expanded Predictions",
        "Fraud_Risk",
        each if [Fraud_Probability] >= 0.7 then "High" 
             else if [Fraud_Probability] >= 0.3 then "Medium" 
             else "Low"
    ),
    
    // Add date hierarchy
    #"Added Date Columns" = Table.AddColumn(
        #"Added Custom",
        "Transaction_Date",
        each DateTime.Date([TransactionDate]),
        type date
    ),
    #"Added Time Column" = Table.AddColumn(
        #"Added Date Columns",
        "Transaction_Time",
        each DateTime.Time([TransactionDate]),
        type time
    ),
    
    // Final cleanup
    #"Removed Columns" = Table.RemoveColumns(
        #"Added Time Column",
        {"Column1"}  // Remove any extra columns
    )
in
    #"Removed Columns"
