[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CoStar_DayStar_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared CoStar_DayStar = let
    // STEP 0: Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file"(#"Sample file")),
    //
    // STEP 1: Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
    //
    // STEP 2: Filter files from /daystardata/ folder with specific CSV name pattern
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and
        not Text.Contains([Folder Path], "_Archive") and 
        not Text.Contains([Folder Path], "_schema_templates") 
        //and Text.Contains([Name], "DayStarDataExtract_AEXHER-77609-Set2_20230201-20250503") // TEST FILE
    ),

    // STEP 3: Filter hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),

    //
    // STEP 4: Apply transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file",each #"Transform file"([Content])),
    //
    // STEP 5: Keep relevant columns
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // STEP 6: Expand using column names from sample file
    ExpandedTableColumn = Table.ExpandTableColumn(RemovedOtherColumns, "Transform file", ColumnNames),
    //
    // Filter reccords that contains MTD in the period row
    RemoveMTDRows = Table.SelectRows(ExpandedTableColumn, each not (Text.Contains(Text.Upper(Text.From([period])), "MTD"))
    ),
    RemoveYTDRows = Table.SelectRows(RemoveMTDRows, each not (Text.Contains(Text.Upper(Text.From([period])), "YTD"))
    ),

    RenamedColumnHeaders = Table.RenameColumns(RemoveYTDRows, {
        
    {"period","stay_date"} ,{"period_ly","stay_date_py"},
    
    {"occ","occ"} ,{"occ_ly","occ_py"} ,{"comp_set_occ","cs_occ"} ,{"comp_set_occ_ly","cs_occ_py"} ,{"occ_%_chg","occ_pct_chg"} ,{"occ_%_chg_ly","occ_pct_chg_py"} ,{"comp_set_occ_%_chg","cs_occ_pct_chg"} ,{"comp_set_occ_%_chg_ly","cs_occ_pct_chg_py"} ,{"occ_index","occ_index"} ,{"occ_index_ly","occ_index_py"} ,{"occ_index_%_chg","occ_index_pct_chg"} ,{"occ_index_%_chg_ly","occ_index_pct_chg_py"} ,{"occ_rank","occ_rank"} ,{"occ_rank_ly","occ_rank_py"} ,{"occ_%_chg_rank","occ_pct_chg_rank"} ,{"occ_%_chg_rank_ly","occ_pct_chg_rank_py"},

    {"adr","adr"} ,{"adr_ly","adr_py"} ,{"comp_set_adr","cs_adr"} ,{"comp_set_adr_ly","cs_adr_py"} ,{"adr_%_chg","adr_pct_chg"} ,{"adr_%_chg_ly","adr_pct_chg_py"} ,{"comp_set_adr_%_chg","cs_adr_pct_chg"} ,{"comp_set_adr_%_chg_ly","cs_adr_pct_chg_py"} ,{"adr_index","adr_index"} ,{"adr_index_ly","adr_index_py"} ,{"adr_index_%_chg","adr_index_pct_chg"} ,{"adr_index_%_chg_ly","adr_index_pct_chg_py"} ,{"adr_rank","adr_rank"} ,{"adr_rank_ly","adr_rank_py"} ,{"adr_%_chg_rank","adr_pct_chg_rank"} ,{"adr_%_chg_rank_ly","adr_pct_chg_rank_py"},

    {"revpar","revpar"} ,{"revpar_ly", "revpar_py"} ,{"comp_set_revpar","cs_revpar"} ,{"comp_set_revpar_ly","cs_revpar_py"} ,{"revpar_%_chg", "revpar_pct_chg"} ,{"revpar_%_chg_ly","revpar_pct_chg_py"} ,{"comp_set_revpar_%_chg","cs_revpar_pct_chg"} ,{"comp_set_revpar_%_chg_ly", "cs_revpar_pct_chg_py"} ,{"revpar_index","revpar_index"} ,{"revpar_index_ly","revpar_index_py"} ,{"revpar_index_%_chg", "revpar_index_pct_chg"} ,{"revpar_index_%_chg_ly","revpar_index_pct_chg_py"} ,{"revpar_rank","revpar_rank"} ,{"revpar_rank_ly","revpar_rank_py"} ,{"revpar_%_chg_rank","revpar_pct_chg_rank"} ,{"revpar_%_chg_rank_ly","revpar_pct_chg_rank_py"},

    {"currency_code","currency_code"} ,{"day_of_week","weekday"} ,{"day_of_week_ly","weekday_py"}

    }),
    //
    ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {
        {"stay_date", type date, "en-US"},  {"stay_date_py", type date, "en-US"}, {"weekday", type text},  {"weekday_py", type text}, 
        
        {"occ", type number},  {"occ_py", type number},  {"cs_occ", type number},  {"cs_occ_py", type number},  {"occ_pct_chg", type number},  {"occ_pct_chg_py", type number},  {"cs_occ_pct_chg", type number},  {"cs_occ_pct_chg_py", type number},  {"occ_index", type number},  {"occ_index_py", type number},  {"occ_index_pct_chg", type number},  {"occ_index_pct_chg_py", type number},  {"occ_rank", type text},  {"occ_rank_py", type text},  {"occ_pct_chg_rank", type text},  {"occ_pct_chg_rank_py", type text},  
        
        {"adr", type number},  {"adr_py", type number},  {"cs_adr", type number},  {"cs_adr_py", type number},  {"adr_pct_chg", type number},  {"adr_pct_chg_py", type number},  {"cs_adr_pct_chg", type number},  {"cs_adr_pct_chg_py", type number},  {"adr_index", type number},  {"adr_index_py", type number},  {"adr_index_pct_chg", type number},  {"adr_index_pct_chg_py", type number},  {"adr_rank", type text},  {"adr_rank_py", type text},  {"adr_pct_chg_rank", type text},  {"adr_pct_chg_rank_py", type text},  
        
        {"revpar", type number},  {"revpar_py", type number},  {"cs_revpar", type number},  {"cs_revpar_py", type number},  {"revpar_pct_chg", type number},  {"revpar_pct_chg_py", type number},  {"cs_revpar_pct_chg", type number},  {"cs_revpar_pct_chg_py", type number},  {"revpar_index", type number},  {"revpar_index_py", type number},  {"revpar_index_pct_chg", type number},  {"revpar_index_pct_chg_py", type number},  {"revpar_rank", type text},  {"revpar_rank_py", type text},  {"revpar_pct_chg_rank", type text},  {"revpar_pct_chg_rank_py", type text}

    }),

    AddedPropertyCode = Table.AddColumn(ChangedColumnType, "property_code", each fxGetPropertyCode([Source.Name]), type text),
    //
    AddedCompsetValue = Table.AddColumn(AddedPropertyCode, "cs_set_id", each fxGetCompsetValue([Source.Name]), type text),

    // STEP 8: Channel key logic
    AddedCompsetLookup = Table.AddColumn(AddedCompsetValue, "compset_lookup", each 
        let
            property_key = Text.Lower(Text.Remove([property_code], {"(", ")", "[", "]"})),
            compset_key = if Text.Trim([cs_set_id]) = "" or [cs_set_id] = null then null else Text.Lower(Text.Remove([cs_set_id], {"(", ")", "[", "]"}))
        in
            if compset_key = null then property_key else property_key & "_" & compset_key, 
        type text
    ),

    // 📓 Check if the mapping table has rows
    MapCompsetExists = try
        Table.HasColumns(SafeMapCompset, "compset_key") and Table.RowCount(SafeMapCompset) > 0 otherwise false,

    // 📓 Conditionally join compset map
    MergedCompsetMap = if MapCompsetExists then 
        Table.NestedJoin(AddedCompsetLookup, {"compset_lookup"}, SafeMapCompset, {"compset_key"},  "map_compset", JoinKind.LeftOuter) 
    else AddedCompsetLookup,

    // 🔒 Fallback for when map_compset doesn't exist
    SafeTypedCompsetMap = Table.TransformColumns(
        MergedCompsetMap,
        {
            {"compset_key", each _, type text}, 
            {"cs_reference", each _, type text}, 
            {"property_name", each _, type text}, 
            {"property_shortname", each _, type text},
            {"census_id", each _, type text},  
            {"brand", each _, type text}, 
            {"physical_capacity", each try Int64.From(_) otherwise 0, Int64.Type},
            {"cs_physical_capacity", each try Int64.From(_) otherwise 0, Int64.Type}
        }
    ),

    // 📓 Conditional final expansion
    ExpandedCompsetMap = if MapCompsetExists then 
        Table.TransformColumnTypes(Table.ExpandTableColumn(
            MergedCompsetMap, "map_compset", { 
                "compset_key", "cs_reference", "property_name", "property_shortname",  "census_id", "brand", "physical_capacity", "cs_physical_capacity" }
                ),
            {
                {"physical_capacity", Int64.Type}, {"cs_physical_capacity", Int64.Type}
            }
        ) else SafeTypedCompsetMap,

    // ✅ Add External Compset Data to Table
    FinalExpandedCompsetMap = ExpandedCompsetMap, 

    // 📆 Add core time columns
    AddMonthName = Table.AddColumn(FinalExpandedCompsetMap, "month_name", each Date.ToText([stay_date], "MMMM"), type text),
    AddCY = Table.AddColumn(AddMonthName, "cy", each Date.ToText([stay_date], "yyyy"), type text),
    AddPY = Table.AddColumn(AddCY, "py", each Date.ToText([stay_date_py], "yyyy"), type text),
    AddMonth = Table.AddColumn(AddPY, "month", each Date.ToText([stay_date], "MM"), type text),
    //AddWeekday = Table.AddColumn(AddMonth, "weekday", each Date.ToText([stay_date], "dddd"), type text),
    //AddWeekdayPY = Table.AddColumn(AddWeekday, "weekday_py", each Date.ToText([stay_date_py], "dddd"), type text),

    // 📆 Add week numbers
    AddWeekNo = Table.AddColumn(AddMonth, "week_no", each Date.WeekOfYear([stay_date]), type number),
    AddWeekNoPY = Table.AddColumn(AddWeekNo, "week_no_py", each Date.WeekOfYear([stay_date_py]), type number),

    // 📆 Add text/date references
    AddStayDateText = Table.AddColumn(AddWeekNoPY, "stay_date_text", each Date.ToText([stay_date], "yyyy-MM-dd"), type text),
    AddStayDateTextPY = Table.AddColumn(AddStayDateText, "stay_date_text_py", each Date.ToText([stay_date_py], "yyyy-MM-dd"), type text),

    // 📆 Add weekday short name and day number
    AddDOW = Table.AddColumn(AddStayDateTextPY, "dow", each Date.ToText([stay_date], "ddd"), type text),
    AddDay = Table.AddColumn(AddDOW, "day", each Date.Day([stay_date]), type number),

    // 📆 RMS calculations
    AddRMSSold = Table.AddColumn(AddDay, "rms", each 
        if [occ] <> null and [physical_capacity] <> null then Number.Round([occ] * [physical_capacity] / 100, 0) else null, type number),

    AddCSRmsSold = Table.AddColumn(AddRMSSold, "cs_rms", each 
        if [cs_occ] <> null and [cs_physical_capacity] <> null then Number.Round([cs_occ] * [cs_physical_capacity] / 100, 0) else null, type number),

    AddRmsSoldPY = Table.AddColumn(AddCSRmsSold, "rms_py", each 
        if [occ_py] <> null and [physical_capacity] <> null then Number.Round([occ_py] * [physical_capacity] / 100, 0) else null, type number),

    AddCSRmsSoldPY = Table.AddColumn(AddRmsSoldPY, "cs_rms_py", each 
        if [cs_occ_py] <> null and [cs_physical_capacity] <> null then Number.Round([cs_occ_py] * [cs_physical_capacity] / 100, 0) else null, type number),

    
    // 📆 REV calculations  
    AddRevenue = Table.AddColumn(AddCSRmsSoldPY, "rev", each 
        if [rms] <> null and [adr] <> null then [rms] * [adr] else null, type number),

    AddCSRevenue = Table.AddColumn(AddRevenue, "cs_rev", each 
        if [cs_rms] <> null and [cs_adr] <> null then [cs_rms] * [cs_adr] else null, type number),

    AddRevenuePY = Table.AddColumn(AddCSRevenue, "rev_py", each 
        if [rms_py] <> null and [adr_py] <> null then [rms_py] * [adr_py] else null, type number),

    AddCSRevenuePY = Table.AddColumn(AddRevenuePY, "cs_rev_py", each 
        if [cs_rms_py] <> null and [cs_adr_py] <> null then [cs_rms_py] * [cs_adr_py] else null, type number),


    // 📆 Add constant day counts
    AddNoDays = Table.AddColumn(AddCSRevenuePY, "no_days", each 1, type number),
    AddNoDaysPY = Table.AddColumn(AddNoDays, "no_days_py", each 1, type number),

    // 📆 Final enforced typing for Fabric Warehouse safety
    FormattedDates = Table.TransformColumnTypes(
        AddNoDaysPY,
        {
            {"stay_date", type date},
            {"stay_date_py", type date},
            {"stay_date_text", type text},
            {"stay_date_text_py", type text}
        }
    ),
    //-- ✅ Added DateTime Columns to Table
    FinalAddedDatetimeColumns = FormattedDates, 
    //
    //-- ✅ Step 11: Add BigQuery Tracking Columns
    AddedBQColumns = Table.AddColumn(
        Table.AddColumn(FinalAddedDatetimeColumns, "sent_to_big_query", each false, type logical),
        "date_sent_to_big_query", each null, type datetime
    ),
    //
    // 🕒 STEP 20: Add timestamp column
    WithIngestedTimestamp = Table.AddColumn(AddedBQColumns, "ingested_timestamp", each DateTime.LocalNow(), type datetime),
    //
    // Final output and preview options
    RemovedRows = Table.RemoveColumns(WithIngestedTimestamp, { 
    //
         "Source.Name", "compset_lookup", "compset_key", "currency_code",
        
        "occ_%_chg_28d", "occ_%_chg_28d_ly", "occ_%_chg_mtd", "occ_%_chg_mtd_ly", "occ_%_chg_rank_mtd", "occ_%_chg_rank_mtd_ly", "occ_28d", "occ_28d_ly", "occ_index_%_chg_28d", "occ_index_%_chg_28d_ly", "occ_index_%_chg_mtd", "occ_index_%_chg_mtd_ly", "occ_index_28d", "occ_index_28d_ly", "occ_index_mtd", "occ_index_mtd_ly", "occ_mtd", "occ_mtd_ly", "occ_rank_mtd", "occ_rank_mtd_ly", "comp_set_occ_%_chg_28d", "comp_set_occ_%_chg_28d_ly", "comp_set_occ_%_chg_mtd", "comp_set_occ_%_chg_mtd_ly", "comp_set_occ_28d", "comp_set_occ_28d_ly", "comp_set_occ_mtd", "comp_set_occ_mtd_ly", 

        "adr_%_chg_28d", "adr_%_chg_28d_ly", "adr_%_chg_mtd", "adr_%_chg_mtd_ly", "adr_%_chg_rank_mtd", "adr_%_chg_rank_mtd_ly", "adr_28d", "adr_28d_ly", "adr_index_%_chg_28d", "adr_index_%_chg_28d_ly", "adr_index_%_chg_mtd", "adr_index_%_chg_mtd_ly", "adr_index_28d", "adr_index_28d_ly", "adr_index_mtd", "adr_index_mtd_ly", "adr_mtd", "adr_mtd_ly", "adr_rank_mtd", "adr_rank_mtd_ly", "comp_set_adr_%_chg_28d", "comp_set_adr_%_chg_28d_ly", "comp_set_adr_%_chg_mtd", "comp_set_adr_%_chg_mtd_ly", "comp_set_adr_28d", "comp_set_adr_28d_ly", "comp_set_adr_mtd", "comp_set_adr_mtd_ly", 
        
        "revpar_%_chg_28d", "revpar_%_chg_28d_ly", "revpar_%_chg_mtd", "revpar_%_chg_mtd_ly", "revpar_%_chg_rank_mtd", "revpar_%_chg_rank_mtd_ly", "revpar_28d", "revpar_28d_ly", "revpar_index_%_chg_28d", "revpar_index_%_chg_28d_ly", "revpar_index_%_chg_mtd", "revpar_index_%_chg_mtd_ly", "revpar_index_28d", "revpar_index_28d_ly", "revpar_index_mtd", "revpar_index_mtd_ly", "revpar_mtd", "revpar_mtd_ly", "revpar_rank_mtd", "revpar_rank_mtd_ly", "comp_set_revpar_%_chg_28d", "comp_set_revpar_%_chg_28d_ly", "comp_set_revpar_%_chg_mtd", "comp_set_revpar_%_chg_mtd_ly", "comp_set_revpar_28d", "comp_set_revpar_28d_ly", "comp_set_revpar_mtd", "comp_set_revpar_mtd_ly"
    //
    }, MissingField.Ignore),
    //
    ColumnOrder = {
    //
        "property_code", "property_name", "property_shortname", "census_id", "cs_set_id", "cs_reference", "brand",
        "month", "month_name", "cy", "py", "weekday", "weekday_py",  "week_no", "week_no_py", "dow", "day", 
        "stay_date_text", "stay_date_text_py", "stay_date", "stay_date_py", 
        "no_days", "no_days_py", "physical_capacity", "cs_physical_capacity",
        
        "rms", "rms_py", "cs_rms", "cs_rms_py",
        "rev", "rev_py", "cs_rev", "cs_rev_py",

        "occ", "occ_py", "cs_occ", "cs_occ_py", "occ_pct_chg", "occ_pct_chg_py", "cs_occ_pct_chg", "cs_occ_pct_chg_py",
        "occ_index", "occ_index_py",  "occ_index_pct_chg", "occ_index_pct_chg_py",
        "occ_rank", "occ_rank_py", "occ_pct_chg_rank", "occ_pct_chg_rank_py",
        
        "adr", "adr_py", "cs_adr", "cs_adr_py",
        "adr_pct_chg", "adr_pct_chg_py", "cs_adr_pct_chg", "cs_adr_pct_chg_py",
        "adr_index", "adr_index_py", "adr_index_pct_chg", "adr_index_pct_chg_py",
        "adr_rank", "adr_rank_py", "adr_pct_chg_rank", "adr_pct_chg_rank_py",
        
        "revpar", "revpar_py", "cs_revpar", "cs_revpar_py",
        "revpar_pct_chg","revpar_pct_chg_py", "cs_revpar_pct_chg", "cs_revpar_pct_chg_py",
        "revpar_index", "revpar_index_py", "revpar_index_pct_chg", "revpar_index_pct_chg_py",
        "revpar_rank", "revpar_rank_py", "revpar_pct_chg_rank", "revpar_pct_chg_rank_py"
    //
    },
    //
    // STEP 10: Reorder only the columns that exist
    AvailableColumns = Table.ColumnNames(RemovedRows),
    ReorderedColumnsList = List.Intersect({ColumnOrder, AvailableColumns}),
    ReorderedTable = Table.ReorderColumns(RemovedRows, ReorderedColumnsList, MissingField.Ignore),
    //
    // STEP 11: Diagnostics
    ErroredRows = Table.SelectRows(ReorderedTable, each List.ContainsAny(Record.FieldValues(_), {"Error"})),
    //
    // 🚫 STEP 25: Remove unsupported columns before Warehouse write
    RemovedLookupKeys = Table.RemoveColumns(ReorderedTable, { "demand_cs_lookup", "demand_cs_key"}, MissingField.Ignore),
    //
    FinalOutput = RemovedLookupKeys
    //
in
    FinalOutput;
shared fxGetPropertyCode = (SourceText as text) as nullable text =>
let
    Keywords = { "CHIAHG", "DENCHM", "MCICRH", "DTWDFH", "PDXTHH", "MSPHEH", "ATLGRA", "TPAHAY", "AEXHER", "FARJAS", "DENPOP", "SEAPOP", "DSMSUR", "SDFANO", "OMAMWH", "MEMHUH", "MSYPON", "EWRTMC"},
    Match = List.First(
        List.Select(Keywords, each Text.Contains(SourceText, _)),
        null
    )
in
    Match;
shared fxGetCompsetValue = (SourceText as text) as nullable text =>
let
    Keywords = {"Set1", "Set2", "Set3", "Set4"},
    Match = List.First(
        List.Select(Keywords, each Text.Contains(SourceText, _)),
        null
    )
in
    Match;
shared #"Transform Sample file" = let
    // Try to read as UTF-8 comma-delimited first
    CsvUTF8 = try Csv.Document(#"Parameter", [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),

    // Fallback to UTF-16 tab-delimited if UTF-8 fails
    Source = if CsvUTF8[HasError] then Csv.Document( #"Parameter",[Delimiter = "#(tab)", Encoding = 1200, QuoteStyle = QuoteStyle.None])
    else
        CsvUTF8[Value],

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    CleanedHeaders = Table.SelectColumns(PromotedHeaders, {"column1", "column2", "column3"}, MissingField.Ignore),
    // Normalize headers
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader)
in
    RenamedHeaders;
shared ColumnNames = let
  ColumnNames = Table.ColumnNames(#"Transform Sample file"),
  #"Convert to table" = Table.FromValue(ColumnNames),
  #"Transform columns" = Table.TransformColumnTypes(#"Convert to table", {{"Value", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Value", null}})
in
  #"Replace errors";
shared #"Sample file" = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
      Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/") and 
      [Extension] = ".csv" and 
      ([Attributes]?[Hidden]? <> true)
  ),
  TemplateFile = Table.SelectRows(FilteredFiles, each Text.Contains([Name], "DayStarDataExport_Schema.csv")),
  FirstFile = try TemplateFile{0}[Content] otherwise error "DayStar Schema template file not found"
in
  FirstFile;
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform file" = (Parameter as binary) => let
    // Try to read as UTF-8 comma-delimited first
    CsvUTF8 = try Csv.Document(#"Parameter", [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),

    // Fallback to UTF-16 tab-delimited if UTF-8 fails
    Source = if CsvUTF8[HasError] then Csv.Document( #"Parameter",[Delimiter = "#(tab)", Encoding = 1200, QuoteStyle = QuoteStyle.None])
    else
        CsvUTF8[Value],

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    CleanedHeaders = Table.SelectColumns(PromotedHeaders, {"column1", "column2", "column3"}, MissingField.Ignore),
    // Normalize headers
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader)
in
    RenamedHeaders;
shared map_compset_gsheet = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1-qF0HFiK0wOt_YJzvUYQfOJIkmI2YU8fqJasl1JajMo/export?format=csv&id=1-qF0HFiK0wOt_YJzvUYQfOJIkmI2YU8fqJasl1JajMo&gid=631820342"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared SafeMapCompset = let
      ExpectedColumns = {
      
            "compset_key",
            "property_code",
            "census_id",
            "cs_no",
            "cs_reference",
            "unqiue_compset_id",
            "property_name",
            "property_shortname",
            "brand",
            "city",
            "state",
            "country",
            "physical_capacity",
            "cs_physical_capacity",
            "open_date",
            "cs_type",
            "cs_owner",
            "compliance_status",
            "delete_on_date",
            "cs_status"
      },
      //
      ActualColumns = try Table.ColumnNames(map_compset_gsheet) otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapCompsetRaw = if SchemaValid 
      then map_compset_gsheet 
      else #table(ExpectedColumns, {}),
      //
      TransformColumns = Table.TransformColumnTypes(SafeMapCompsetRaw, {

            {"compset_key", type text},
            {"property_code", type text},
            {"census_id", type text},
            {"cs_no", type text},
            {"cs_reference", type text},
            {"unqiue_compset_id", type text},
            {"property_name", type text},
            {"property_shortname", type text},
            {"brand", type text},
            {"city", type text},
            {"state", type text},
            {"country", type text},
            {"physical_capacity", Int64.Type},
            {"cs_physical_capacity", Int64.Type},
            {"open_date", type text},
            {"cs_type", type text},
            {"cs_owner", type text},
            {"compliance_status", type text},
            {"delete_on_date", type date},
            {"cs_status", type text}
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {

            {"compset_key", null},
            {"property_code", null},
            {"census_id", null},
            {"cs_no", null},
            {"cs_reference", null},
            {"unqiue_compset_id", null},
            {"property_name", null},
            {"property_shortname", null},
            {"brand", null},
            {"city", null},
            {"state", null},
            {"country", null},
            {"physical_capacity", null},
            {"cs_physical_capacity", null},
            {"open_date", null},
            {"cs_type", null},
            {"cs_owner", null},
            {"compliance_status", null},
            {"delete_on_date", null},
            {"cs_status", null}
      })
in
      ReplaceErrors;
shared SafeMapCompsetSchemaFallback = let
  EmptyMapCompsetTable = #table(
  //
      {
      //
            "compset_key",
            "property_code",
            "census_id",
            "cs_no",
            "cs_reference",
            "unqiue_compset_id",
            "property_name",
            "property_shortname",
            "brand",
            "city",
            "state",
            "country",
            "physical_capacity",
            "cs_physical_capacity",
            "open_date",
            "cs_type",
            "cs_owner",
            "compliance_status",
            "delete_on_date",
            "cs_status"
      //
      },
      {}
    ),
  TransformColumns = Table.TransformColumnTypes(EmptyMapCompsetTable, {
  //
          {"compset_key", type text},
          {"property_code", type text},
          {"census_id", type text},
          {"cs_no", type text},
          {"cs_reference", type text},
          {"unqiue_compset_id", type text},
          {"property_name", type text},
          {"property_shortname", type text},
          {"brand", type text},
          {"city", type text},
          {"state", type text},
          {"country", type text},
          {"physical_capacity", Int64.Type},
          {"cs_physical_capacity", Int64.Type},
          {"open_date", type text},
          {"cs_type", type text},
          {"cs_owner", type text},
          {"compliance_status", type text},
          {"delete_on_date", type date},
          {"cs_status", type text}
//
  }),
  //
  ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {
  //
          {"compset_key", null},
          {"property_code", null},
          {"census_id", null},
          {"cs_no", null},
          {"cs_reference", null},
          {"unqiue_compset_id", null},
          {"property_name", null},
          {"property_shortname", null},
          {"brand", null},
          {"city", null},
          {"state", null},
          {"country", null},
          {"physical_capacity", null},
          {"cs_physical_capacity", null},
          {"open_date", null},
          {"cs_type", null},
          {"cs_owner", null},
          {"compliance_status", null},
          {"delete_on_date", null},
          {"cs_status", null}
  //
  })
in
  ReplaceErrors;
shared GetColumnNames = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  // STEP 2: Filter for PMS folder and CSV files
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file (2)"(SampleContent),
  ColumnNames = Table.ColumnNames(TransformedSample),
  Quoted = List.Transform(ColumnNames, each """" & _ & ""","),
  OutputText = Text.Combine(Quoted, "#(cr)#(lf)")
//  ConvertToTable = Table.FromValue(OutputText)
in
  OutputText;
shared GetColumnNames_Table = let
  // Combine already-transformed tables
  GetTables = Table.Combine({#"map_compset_gsheet (2)"}),
  // Use the first actual data row/table
  SampleRow = Table.FirstN(GetTables, 1),
  // Convert to a table for schema analysis
  TransformedSample = SampleRow,
  ColumnSchema = Table.Schema(TransformedSample),
  ColumnNames = Table.ColumnNames(TransformedSample),
  Quoted = List.Transform(ColumnNames, each """" & _ & ""","),
  OutputText = Text.Combine(Quoted, "#(cr)#(lf)")
in
  OutputText;
shared GetColumnTypes = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  // STEP 2: Filter for PMS folder and CSV files
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file (2)"(SampleContent),
  // Convert to a table for schema analysis
  ColumnSchema = Table.Schema(TransformedSample),
  TypePairs = List.Transform(
        Table.ToRecords(ColumnSchema), 
        each "    {""" & [Name] & """, type " & Text.Lower(Text.From([Kind])) & "}"
    ),
  CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),
  OutputText = "ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {" & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & CombinedTypeText & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & "}),"
in
  OutputText;
shared GetColumnTypes_Table = let
  // Combine already-transformed tables
  GetTables = Table.Combine({#"map_compset_gsheet (2)"}),
  // Use the first actual data row/table
  SampleRow = Table.FirstN(GetTables, 1),
  // Convert to a table for schema analysis
  TransformedSample = SampleRow,
  ColumnSchema = Table.Schema(TransformedSample),
  TypePairs = List.Transform(
        Table.ToRecords(ColumnSchema), 
        each "    {""" & [Name] & """, type " & Text.Lower(Text.From([Kind])) & "}"
    ),
  CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),
  RunFunction = "#(cr)#(lf)" & CombinedTypeText & "#(cr)#(lf)",
  OutputText = RunFunction
in
OutputText;
shared ReplaceColumnNames = let
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  // STEP 2: Filter for PMS folder and CSV files
  FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and 
        ([Attributes]?[Hidden]? <> true)
    ),
  FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),
  SampleFile = Table.FirstN(FilteredFiles, 1){0},
  SampleContent = SampleFile[Content],
  TransformedSample = #"Transform file (2)"(SampleContent),
  ColumnNames = Table.ColumnNames(TransformedSample),
  RenamePairs = List.Transform(ColumnNames, each "    {""" & _ & """, """ & _ & """}"),
  RenameText = Text.Combine(RenamePairs, "," & "#(cr)#(lf)"),
  OutputText = "// 🚀 STEP 3: Rename Key Columns" & "#(cr)#(lf)" & "#(cr)#(lf)" & "RenamedColumns = Table.RenameColumns(FinalRemovedColumns, {" & "#(cr)#(lf)" & RenameText & "#(cr)#(lf)" & "}),"
//  ConvertToTable = Table.FromValue(OutputText)
in
  OutputText;
shared RenameColumnName_Table = let
  // Combine already-transformed tables
  GetTables = Table.Combine({CoStarData_DayStar}),
  // Use the first actual data row/table
  SampleRow = Table.FirstN(GetTables, 1),
  // Convert to a table for schema analysis
  TransformedSample = SampleRow,
  ColumnSchema = Table.Schema(TransformedSample),
  RenamePairs = List.Transform(ColumnSchema[Name], each "    {""" & _ & """, """ & _ & """}"),
  RenameText = Text.Combine(RenamePairs, "," & "#(cr)#(lf)"),
  OutputText = "// 🚀 STEP 3: Rename Key Columns" & "#(cr)#(lf)" & "#(cr)#(lf)" & "RenamedColumns = Table.RenameColumns(FinalRemovedColumns, {" & "#(cr)#(lf)" & RenameText & "#(cr)#(lf)" & "}),"
//  ConvertToTable = Table.FromValue(OutputText)
in
  OutputText;
shared GetColumnNullTypes_Table = let
    // Combine tables
    GetTables = Table.Combine({#"map_compset_gsheet (2)"}),

    // Use first row to get structure
    SampleRow = Table.FirstN(GetTables, 1),

    // Get column names
    ColumnNames = Table.ColumnNames(SampleRow),

    // Create {"col", null} lines with indentation
    QuotedLines = List.Transform(ColumnNames, each "    {""" & _ & """, null}"),

    // Combine into full text block for use in query
    CombinedText = Text.Combine(QuotedLines, "," & "#(cr)#(lf)"),

    // Wrap in desired structure (as you'd paste into your M code)
    OutputText = CombinedText
in
    OutputText;
shared DuplicateRowCheck = let
  // STEP 1: Load your table
  Source = CoStarData_DayStar,

  // STEP 2: Group by the keys and count rows in each group
  Grouped = Table.Group(Source, {"property_code", "cs_set_id", "stay_date"}, {
    {"Count", each Table.RowCount(_), Int64.Type}
  }),

  // STEP 3: Filter for groups with more than 1 row (i.e., duplicates)
  DuplicatesOnly = Table.SelectRows(Grouped, each [Count] > 1),

  // STEP 4: Join original table with duplicates to get full details
  Merged = Table.NestedJoin(Source, {"property_code", "cs_set_id", "stay_date"},
                            DuplicatesOnly, {"property_code", "cs_set_id", "stay_date"},
                            "DuplicateMatch", JoinKind.Inner),

  // STEP 5: Expand to return the original rows only
  Result = Table.RemoveColumns(Merged, {"DuplicateMatch"})
in
  Result;
shared GetRawColumnNames_Table = let
    // STEP 0: Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)")),
    //
    // STEP 1: Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
    //
    // STEP 2: Filter files from /daystardata/ folder with specific CSV name pattern
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and
        not Text.Contains([Folder Path], "_Archive") and 
        not Text.Contains([Folder Path], "_schema_templates") and 
        Text.Contains([Name], "DayStarDataExport_AEXHER-77609_Set2_20250427-20250503")
    ),

    // STEP 3: Filter hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),

    //
    // STEP 4: Apply transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file",each #"Transform file (2)"([Content])),
    //
    // STEP 5: Keep relevant columns
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // STEP 6: Expand using column names from sample file
    ExpandedTableColumn = Table.ExpandTableColumn(RemovedOtherColumns, "Transform file", ColumnNames),
    //
    RenamedColumnHeaders = Table.RenameColumns(ExpandedTableColumn, {

    {"period","stay_date"} ,{"period_ly","stay_date_py"},
    {"occ","occ"} ,{"occ_ly","occ_py"} ,{"comp_set_occ","cs_occ"} ,{"comp_set_occ_ly","cs_occ_py"} ,{"occ_%_chg","occ_pct_chg"} ,{"occ_%_chg_ly","occ_pct_chg_py"} ,{"comp_set_occ_%_chg","cs_occ_pct_chg"} ,{"comp_set_occ_%_chg_ly","cs_occ_pct_chg_py"} ,{"occ_index","occ_index"} ,{"occ_index_ly","occ_index_py"} ,{"occ_index_%_chg","occ_index_pct_chg"} ,{"occ_index_%_chg_ly","occ_index_pct_chg_py"} ,{"occ_rank","occ_rank"} ,{"occ_rank_ly","occ_rank_py"} ,{"occ_%_chg_rank","occ_pct_chg_rank"} ,{"occ_%_chg_rank_ly","occ_pct_chg_rank_py"},

    {"adr","adr"} ,{"adr_ly","adr_py"} ,{"comp_set_adr","cs_adr"} ,{"comp_set_adr_ly","cs_adr_py"} ,{"adr_%_chg","adr_pct_chg"} ,{"adr_%_chg_ly","adr_pct_chg_py"} ,{"comp_set_adr_%_chg","cs_adr_pct_chg"} ,{"comp_set_adr_%_chg_ly","cs_adr_pct_chg_py"} ,{"adr_index","adr_index"} ,{"adr_index_ly","adr_index_py"} ,{"adr_index_%_chg","adr_index_pct_chg"} ,{"adr_index_%_chg_ly","adr_index_pct_chg_py"} ,{"adr_rank","adr_rank"} ,{"adr_rank_ly","adr_rank_py"} ,{"adr_%_chg_rank","adr_pct_chg_rank"} ,{"adr_%_chg_rank_ly","adr_pct_chg_rank_py"},

    {"revpar","revpar"} ,{"revpar_ly", "revpar_py"} ,{"comp_set_revpar","cs_revpar"} ,{"comp_set_revpar_ly","cs_revpar_py"} ,{"revpar_%_chg", "revpar_pct_chg"} ,{"revpar_%_chg_ly","revpar_pct_chg_py"} ,{"comp_set_revpar_%_chg","cs_revpar_pct_chg"} ,{"comp_set_revpar_%_chg_ly", "cs_revpar_pct_chg_py"} ,{"revpar_index","revpar_index"} ,{"revpar_index_ly","revpar_index_py"} ,{"revpar_index_%_chg", "revpar_index_pct_chg"} ,{"revpar_index_%_chg_ly","revpar_index_pct_chg_py"} ,{"revpar_rank","revpar_rank"} ,{"revpar_rank_ly","revpar_rank_py"} ,{"revpar_%_chg_rank","revpar_pct_chg_rank"} ,{"revpar_%_chg_rank_ly","revpar_pct_chg_rank_py"},

    {"currency_code","currency_code"} ,{"day_of_week","day_of_week"} ,{"day_of_week_ly","day_of_week_py"}

    }),
    AddedPropertyCode = Table.AddColumn(ExpandedTableColumn, "property_code", each #"fxGetPropertyCode (2)"([file_name])),
    //
    AddedCompsetValue = Table.AddColumn(AddedPropertyCode, "cs_no", each #"fxGetCompsetValue (2)"([file_name])),
    // Final output and preview options
    RemovedRows = Table.RemoveColumns(AddedCompsetValue, {
     
         "Source.Name", "compset_lookup", "compset_key", "currency_code",
        
        "occ_%_chg_28d", "occ_%_chg_28d_ly", "occ_%_chg_mtd", "occ_%_chg_mtd_ly", "occ_%_chg_rank_mtd", "occ_%_chg_rank_mtd_ly", "occ_28d", "occ_28d_ly", "occ_index_%_chg_28d", "occ_index_%_chg_28d_ly", "occ_index_%_chg_mtd", "occ_index_%_chg_mtd_ly", "occ_index_28d", "occ_index_28d_ly", "occ_index_mtd", "occ_index_mtd_ly", "occ_mtd", "occ_mtd_ly", "occ_rank_mtd", "occ_rank_mtd_ly", "comp_set_occ_%_chg_28d", "comp_set_occ_%_chg_28d_ly", "comp_set_occ_%_chg_mtd", "comp_set_occ_%_chg_mtd_ly", "comp_set_occ_28d", "comp_set_occ_28d_ly", "comp_set_occ_mtd", "comp_set_occ_mtd_ly", 

        "adr_%_chg_28d", "adr_%_chg_28d_ly", "adr_%_chg_mtd", "adr_%_chg_mtd_ly", "adr_%_chg_rank_mtd", "adr_%_chg_rank_mtd_ly", "adr_28d", "adr_28d_ly", "adr_index_%_chg_28d", "adr_index_%_chg_28d_ly", "adr_index_%_chg_mtd", "adr_index_%_chg_mtd_ly", "adr_index_28d", "adr_index_28d_ly", "adr_index_mtd", "adr_index_mtd_ly", "adr_mtd", "adr_mtd_ly", "adr_rank_mtd", "adr_rank_mtd_ly", "comp_set_adr_%_chg_28d", "comp_set_adr_%_chg_28d_ly", "comp_set_adr_%_chg_mtd", "comp_set_adr_%_chg_mtd_ly", "comp_set_adr_28d", "comp_set_adr_28d_ly", "comp_set_adr_mtd", "comp_set_adr_mtd_ly", 
        
        "revpar_%_chg_28d", "revpar_%_chg_28d_ly", "revpar_%_chg_mtd", "revpar_%_chg_mtd_ly", "revpar_%_chg_rank_mtd", "revpar_%_chg_rank_mtd_ly", "revpar_28d", "revpar_28d_ly", "revpar_index_%_chg_28d", "revpar_index_%_chg_28d_ly", "revpar_index_%_chg_mtd", "revpar_index_%_chg_mtd_ly", "revpar_index_28d", "revpar_index_28d_ly", "revpar_index_mtd", "revpar_index_mtd_ly", "revpar_mtd", "revpar_mtd_ly", "revpar_rank_mtd", "revpar_rank_mtd_ly", "comp_set_revpar_%_chg_28d", "comp_set_revpar_%_chg_28d_ly", "comp_set_revpar_%_chg_mtd", "comp_set_revpar_%_chg_mtd_ly", "comp_set_revpar_28d", "comp_set_revpar_28d_ly", "comp_set_revpar_mtd", "comp_set_revpar_mtd_ly"
        
    }, MissingField.Ignore),



    TransformedExpandedTableColumn = RemovedRows,

    // STEP 0: Load a sample table — change this to your actual query/table
    SampleTable = TransformedExpandedTableColumn,  // <-- replace with your actual query name or table step

    // STEP 1: Get schema of the sample table
    ColumnSchema = Table.Schema(SampleTable),


      // STEP 2: Transform schema into formatted type lines
      TypePairs = List.Transform(
          Table.ToRecords(ColumnSchema), 
          each "    {""" & [Name] & """, type " & Text.Lower(Text.From([Kind])) & "}"
      ),
      CombinedTypeText = Text.Combine(TypePairs, "," & "#(cr)#(lf)"),
      
      // STEP 4: Wrap into a ready-to-copy M script
      OutputText = "ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {" & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & CombinedTypeText & "#(cr)#(lf)" & "    //" & "#(cr)#(lf)" & "}),"
      in
      OutputText;
shared TestFIle = let
    // STEP 0: Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)")),
    //
    // STEP 1: Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
    //
    // STEP 2: Filter files from /daystardata/ folder with specific CSV name pattern
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and
        not Text.Contains([Folder Path], "_Archive") and 
        not Text.Contains([Folder Path], "_schema_templates") and 
        Text.Contains([Name], "DayStarDataExport_AEXHER-77609_Set2_20250427-20250503")
    ),

    // STEP 3: Filter hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),

    //
    // STEP 4: Apply transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file",each #"Transform file (2)"([Content])),
    //
    // STEP 5: Keep relevant columns
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // STEP 6: Expand using column names from sample file
    ExpandedTableColumn = Table.ExpandTableColumn(RemovedOtherColumns, "Transform file", ColumnNames),
    //
   
    AddedPropertyCode = Table.AddColumn(ExpandedTableColumn, "property_code", each #"fxGetPropertyCode (2)"([Source.Name])),
    //
    AddedCompSetNo = Table.AddColumn(AddedPropertyCode, "cs_no", each #"fxGetCompsetValue (2)"([Source.Name])),

FinalOutput = AddedCompSetNo
in
FinalOutput;
shared #"Sample file (2)" = let
  Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
  FilteredFiles = Table.SelectRows(Source, each 
      Text.Contains(Text.Lower([Folder Path]), "/_schema_templates/") and 
      [Extension] = ".csv" and 
      ([Attributes]?[Hidden]? <> true)
  ),
  TemplateFile = Table.SelectRows(FilteredFiles, each Text.Contains([Name], "DayStarDataExport_Schema.csv")),
  FirstFile = try TemplateFile{0}[Content] otherwise error "DayStar Schema template file not found"
in
  FirstFile;
shared #"Transform file (2)" = (Parameter as binary) => let
    // Try to read as UTF-8 comma-delimited first
    CsvUTF8 = try Csv.Document(#"Parameter", [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None]),

    // Fallback to UTF-16 tab-delimited if UTF-8 fails
    Source = if CsvUTF8[HasError] then Csv.Document( #"Parameter",[Delimiter = "#(tab)", Encoding = 1200, QuoteStyle = QuoteStyle.None])
    else
        CsvUTF8[Value],

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    CleanedHeaders = Table.SelectColumns(PromotedHeaders, {"column1", "column2", "column3"}, MissingField.Ignore),
    // Normalize headers
    NormalizeHeader = (columnName as text) as text =>
        Text.Lower(Text.Replace(columnName, " ", "_")),

    RenamedHeaders = Table.TransformColumnNames(PromotedHeaders, NormalizeHeader)
in
    RenamedHeaders;
shared #"map_compset_gsheet (2)" = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1-qF0HFiK0wOt_YJzvUYQfOJIkmI2YU8fqJasl1JajMo/export?format=csv&id=1-qF0HFiK0wOt_YJzvUYQfOJIkmI2YU8fqJasl1JajMo&gid=631820342"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    #"Promoted Headers";
shared #"SafeMapCompset (2)" = let
      ExpectedColumns = {
      
            "compset_key",
            "property_code",
            "census_id",
            "cs_no",
            "cs_reference",
            "unqiue_compset_id",
            "property_name",
            "property_shortname",
            "brand",
            "city",
            "state",
            "country",
            "physical_capacity",
            "cs_physical_capacity",
            "open_date",
            "cs_type",
            "cs_owner",
            "compliance_status",
            "delete_on_date",
            "cs_status"
      },
      //
      ActualColumns = try Table.ColumnNames(#"map_compset_gsheet (2)") otherwise {},
      SchemaValid = List.Difference(ExpectedColumns, ActualColumns) = {},

      SafeMapCompsetRaw = if SchemaValid 
      then #"map_compset_gsheet (2)" 
      else #table(ExpectedColumns, {}),
      //
      TransformColumns = Table.TransformColumnTypes(SafeMapCompsetRaw, {

            {"compset_key", type text},
            {"property_code", type text},
            {"census_id", type text},
            {"cs_no", type text},
            {"cs_reference", type text},
            {"unqiue_compset_id", type text},
            {"property_name", type text},
            {"property_shortname", type text},
            {"brand", type text},
            {"city", type text},
            {"state", type text},
            {"country", type text},
            {"physical_capacity", Int64.Type},
            {"cs_physical_capacity", Int64.Type},
            {"open_date", type text},
            {"cs_type", type text},
            {"cs_owner", type text},
            {"compliance_status", type text},
            {"delete_on_date", type date},
            {"cs_status", type text}
      }),
      //
      ReplaceErrors = Table.ReplaceErrorValues(TransformColumns, {

            {"compset_key", null},
            {"property_code", null},
            {"census_id", null},
            {"cs_no", null},
            {"cs_reference", null},
            {"unqiue_compset_id", null},
            {"property_name", null},
            {"property_shortname", null},
            {"brand", null},
            {"city", null},
            {"state", null},
            {"country", null},
            {"physical_capacity", null},
            {"cs_physical_capacity", null},
            {"open_date", null},
            {"cs_type", null},
            {"cs_owner", null},
            {"compliance_status", null},
            {"delete_on_date", null},
            {"cs_status", null}
      })
in
      ReplaceErrors;
shared #"fxGetPropertyCode (2)" = (SourceText as text) as nullable text =>
let
    Keywords = { "CHIAHG", "DENCHM", "MCICRH", "DTWDFH", "PDXTHH", "MSPHEH", "ATLGRA", "TPAHAY", "AEXHER", "FARJAS", "DENPOP", "SEAPOP", "DSMSUR", "SDFANO", "OMAMWH", "MEMHUH", "MSYPON", "EWRTMC"},
    Match = List.First(
        List.Select(Keywords, each Text.Contains(SourceText, _)),
        null
    )
in
    Match;
shared #"fxGetCompsetValue (2)" = (SourceText as text) as nullable text =>
let
    Keywords = {"Set1", "Set2", "Set3", "Set4"},
    Match = List.First(
        List.Select(Keywords, each Text.Contains(SourceText, _)),
        null
    )
in
    Match;
shared CoStarData_DayStar = let
    // STEP 0: Get column names from sample file
    ColumnNames = Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)")),
    //
    // STEP 1: Load files from SharePoint
    Source = SharePoint.Files("https://revrebelhub.sharepoint.com/sites/costar", [ApiVersion = 15]),
    //
    // STEP 2: Filter files from /daystardata/ folder with specific CSV name pattern
    FilteredFiles = Table.SelectRows(Source, each 
        Text.Contains(Text.Lower([Folder Path]), "/_database/") and 
        [Extension] = ".csv" and
        not Text.Contains([Folder Path], "_Archive") and 
        not Text.Contains([Folder Path], "_schema_templates") 
        //and Text.Contains([Name], "DayStarDataExtract_AEXHER-77609-Set2_20230201-20250503") // TEST FILE
    ),

    // STEP 3: Filter hidden files
    FilteredHiddenFiles = Table.SelectRows(FilteredFiles, each [Attributes]?[Hidden]? <> true),

    //
    // STEP 4: Apply transform function
    InvokeCustomFunction = Table.AddColumn(FilteredHiddenFiles, "Transform file",each #"Transform file (2)"([Content])),
    //
    // STEP 5: Keep relevant columns
    RenamedColumns = Table.RenameColumns(InvokeCustomFunction, {{"Name", "Source.Name"}}),
    RemovedOtherColumns = Table.SelectColumns(RenamedColumns, {"Source.Name", "Transform file"}),
    //
    // STEP 6: Expand using column names from sample file
    ExpandedTableColumn = Table.ExpandTableColumn(RemovedOtherColumns, "Transform file", ColumnNames),
    //
    // Filter reccords that contains MTD in the period row
    RemoveMTDRows = Table.SelectRows(ExpandedTableColumn, each not (Text.Contains(Text.Upper(Text.From([period])), "MTD"))
    ),
    RemoveYTDRows = Table.SelectRows(RemoveMTDRows, each not (Text.Contains(Text.Upper(Text.From([period])), "YTD"))
    ),

    RenamedColumnHeaders = Table.RenameColumns(RemoveYTDRows, {
        
    {"period","stay_date"} ,{"period_ly","stay_date_py"},
    
    {"occ","occ"} ,{"occ_ly","occ_py"} ,{"comp_set_occ","cs_occ"} ,{"comp_set_occ_ly","cs_occ_py"} ,{"occ_%_chg","occ_pct_chg"} ,{"occ_%_chg_ly","occ_pct_chg_py"} ,{"comp_set_occ_%_chg","cs_occ_pct_chg"} ,{"comp_set_occ_%_chg_ly","cs_occ_pct_chg_py"} ,{"occ_index","occ_index"} ,{"occ_index_ly","occ_index_py"} ,{"occ_index_%_chg","occ_index_pct_chg"} ,{"occ_index_%_chg_ly","occ_index_pct_chg_py"} ,{"occ_rank","occ_rank"} ,{"occ_rank_ly","occ_rank_py"} ,{"occ_%_chg_rank","occ_pct_chg_rank"} ,{"occ_%_chg_rank_ly","occ_pct_chg_rank_py"},

    {"adr","adr"} ,{"adr_ly","adr_py"} ,{"comp_set_adr","cs_adr"} ,{"comp_set_adr_ly","cs_adr_py"} ,{"adr_%_chg","adr_pct_chg"} ,{"adr_%_chg_ly","adr_pct_chg_py"} ,{"comp_set_adr_%_chg","cs_adr_pct_chg"} ,{"comp_set_adr_%_chg_ly","cs_adr_pct_chg_py"} ,{"adr_index","adr_index"} ,{"adr_index_ly","adr_index_py"} ,{"adr_index_%_chg","adr_index_pct_chg"} ,{"adr_index_%_chg_ly","adr_index_pct_chg_py"} ,{"adr_rank","adr_rank"} ,{"adr_rank_ly","adr_rank_py"} ,{"adr_%_chg_rank","adr_pct_chg_rank"} ,{"adr_%_chg_rank_ly","adr_pct_chg_rank_py"},

    {"revpar","revpar"} ,{"revpar_ly", "revpar_py"} ,{"comp_set_revpar","cs_revpar"} ,{"comp_set_revpar_ly","cs_revpar_py"} ,{"revpar_%_chg", "revpar_pct_chg"} ,{"revpar_%_chg_ly","revpar_pct_chg_py"} ,{"comp_set_revpar_%_chg","cs_revpar_pct_chg"} ,{"comp_set_revpar_%_chg_ly", "cs_revpar_pct_chg_py"} ,{"revpar_index","revpar_index"} ,{"revpar_index_ly","revpar_index_py"} ,{"revpar_index_%_chg", "revpar_index_pct_chg"} ,{"revpar_index_%_chg_ly","revpar_index_pct_chg_py"} ,{"revpar_rank","revpar_rank"} ,{"revpar_rank_ly","revpar_rank_py"} ,{"revpar_%_chg_rank","revpar_pct_chg_rank"} ,{"revpar_%_chg_rank_ly","revpar_pct_chg_rank_py"},

    {"currency_code","currency_code"} ,{"day_of_week","weekday"} ,{"day_of_week_ly","weekday_py"}

    }),
    //
    ChangedColumnType = Table.TransformColumnTypes(RenamedColumnHeaders, {
        {"stay_date", type date, "en-US"},  {"stay_date_py", type date, "en-US"}, {"weekday", type text},  {"weekday_py", type text}, 
        
        {"occ", type number},  {"occ_py", type number},  {"cs_occ", type number},  {"cs_occ_py", type number},  {"occ_pct_chg", type number},  {"occ_pct_chg_py", type number},  {"cs_occ_pct_chg", type number},  {"cs_occ_pct_chg_py", type number},  {"occ_index", type number},  {"occ_index_py", type number},  {"occ_index_pct_chg", type number},  {"occ_index_pct_chg_py", type number},  {"occ_rank", type text},  {"occ_rank_py", type text},  {"occ_pct_chg_rank", type text},  {"occ_pct_chg_rank_py", type text},  
        
        {"adr", type number},  {"adr_py", type number},  {"cs_adr", type number},  {"cs_adr_py", type number},  {"adr_pct_chg", type number},  {"adr_pct_chg_py", type number},  {"cs_adr_pct_chg", type number},  {"cs_adr_pct_chg_py", type number},  {"adr_index", type number},  {"adr_index_py", type number},  {"adr_index_pct_chg", type number},  {"adr_index_pct_chg_py", type number},  {"adr_rank", type text},  {"adr_rank_py", type text},  {"adr_pct_chg_rank", type text},  {"adr_pct_chg_rank_py", type text},  
        
        {"revpar", type number},  {"revpar_py", type number},  {"cs_revpar", type number},  {"cs_revpar_py", type number},  {"revpar_pct_chg", type number},  {"revpar_pct_chg_py", type number},  {"cs_revpar_pct_chg", type number},  {"cs_revpar_pct_chg_py", type number},  {"revpar_index", type number},  {"revpar_index_py", type number},  {"revpar_index_pct_chg", type number},  {"revpar_index_pct_chg_py", type number},  {"revpar_rank", type text},  {"revpar_rank_py", type text},  {"revpar_pct_chg_rank", type text},  {"revpar_pct_chg_rank_py", type text}

    }),

    AddedPropertyCode = Table.AddColumn(ChangedColumnType, "property_code", each #"fxGetPropertyCode (2)"([Source.Name]), type text),
    //
    AddedCompsetValue = Table.AddColumn(AddedPropertyCode, "cs_set_id", each #"fxGetCompsetValue (2)"([Source.Name]), type text),

    // STEP 8: Channel key logic
    AddedCompsetLookup = Table.AddColumn(AddedCompsetValue, "compset_lookup", each 
        let
            property_key = Text.Lower(Text.Remove([property_code], {"(", ")", "[", "]"})),
            compset_key = if Text.Trim([cs_set_id]) = "" or [cs_set_id] = null then null else Text.Lower(Text.Remove([cs_set_id], {"(", ")", "[", "]"}))
        in
            if compset_key = null then property_key else property_key & "_" & compset_key, 
        type text
    ),

    // 📓 Check if the mapping table has rows
    MapCompsetExists = try
        Table.HasColumns(#"SafeMapCompset (2)", "compset_key") and Table.RowCount(#"SafeMapCompset (2)") > 0 otherwise false,

    // 📓 Conditionally join compset map
    MergedCompsetMap = if MapCompsetExists then 
        Table.NestedJoin(AddedCompsetLookup, {"compset_lookup"}, #"SafeMapCompset (2)", {"compset_key"},  "map_compset", JoinKind.LeftOuter) 
    else AddedCompsetLookup,

    // 🔒 Fallback for when map_compset doesn't exist
    SafeTypedCompsetMap = Table.TransformColumns(
        MergedCompsetMap,
        {
            {"compset_key", each _, type text}, 
            {"cs_reference", each _, type text}, 
            {"property_name", each _, type text}, 
            {"property_shortname", each _, type text},
            {"census_id", each _, type text},  
            {"brand", each _, type text}, 
            {"physical_capacity", each try Int64.From(_) otherwise 0, Int64.Type},
            {"cs_physical_capacity", each try Int64.From(_) otherwise 0, Int64.Type}
        }
    ),

    // 📓 Conditional final expansion
    ExpandedCompsetMap = if MapCompsetExists then 
        Table.TransformColumnTypes(Table.ExpandTableColumn(
            MergedCompsetMap, "map_compset", { 
                "compset_key", "cs_reference", "property_name", "property_shortname",  "census_id", "brand", "physical_capacity", "cs_physical_capacity" }
                ),
            {
                {"physical_capacity", Int64.Type}, {"cs_physical_capacity", Int64.Type}
            }
        ) else SafeTypedCompsetMap,

    // ✅ Add External Compset Data to Table
    FinalExpandedCompsetMap = ExpandedCompsetMap, 

    // 📆 Add core time columns
    AddMonthName = Table.AddColumn(FinalExpandedCompsetMap, "month_name", each Date.ToText([stay_date], "MMMM"), type text),
    AddCY = Table.AddColumn(AddMonthName, "cy", each Date.ToText([stay_date], "yyyy"), type text),
    AddPY = Table.AddColumn(AddCY, "py", each Date.ToText([stay_date_py], "yyyy"), type text),
    AddMonth = Table.AddColumn(AddPY, "month", each Date.ToText([stay_date], "MM"), type text),
    //AddWeekday = Table.AddColumn(AddMonth, "weekday", each Date.ToText([stay_date], "dddd"), type text),
    //AddWeekdayPY = Table.AddColumn(AddWeekday, "weekday_py", each Date.ToText([stay_date_py], "dddd"), type text),

    // 📆 Add week numbers
    AddWeekNo = Table.AddColumn(AddMonth, "week_no", each Date.WeekOfYear([stay_date]), type number),
    AddWeekNoPY = Table.AddColumn(AddWeekNo, "week_no_py", each Date.WeekOfYear([stay_date_py]), type number),

    // 📆 Add text/date references
    AddStayDateText = Table.AddColumn(AddWeekNoPY, "stay_date_text", each Date.ToText([stay_date], "yyyy-MM-dd"), type text),
    AddStayDateTextPY = Table.AddColumn(AddStayDateText, "stay_date_text_py", each Date.ToText([stay_date_py], "yyyy-MM-dd"), type text),

    // 📆 Add weekday short name and day number
    AddDOW = Table.AddColumn(AddStayDateTextPY, "dow", each Date.ToText([stay_date], "ddd"), type text),
    AddDay = Table.AddColumn(AddDOW, "day", each Date.Day([stay_date]), type number),

    // 📆 RMS calculations
    AddRMSSold = Table.AddColumn(AddDay, "rms", each 
        if [occ] <> null and [physical_capacity] <> null then Number.Round([occ] * [physical_capacity] / 100, 0) else null, type number),

    AddCSRmsSold = Table.AddColumn(AddRMSSold, "cs_rms", each 
        if [cs_occ] <> null and [cs_physical_capacity] <> null then Number.Round([cs_occ] * [cs_physical_capacity] / 100, 0) else null, type number),

    AddRmsSoldPY = Table.AddColumn(AddCSRmsSold, "rms_py", each 
        if [occ_py] <> null and [physical_capacity] <> null then Number.Round([occ_py] * [physical_capacity] / 100, 0) else null, type number),

    AddCSRmsSoldPY = Table.AddColumn(AddRmsSoldPY, "cs_rms_py", each 
        if [cs_occ_py] <> null and [cs_physical_capacity] <> null then Number.Round([cs_occ_py] * [cs_physical_capacity] / 100, 0) else null, type number),

    
    // 📆 REV calculations  
    AddRevenue = Table.AddColumn(AddCSRmsSoldPY, "rev", each 
        if [rms] <> null and [adr] <> null then [rms] * [adr] else null, type number),

    AddCSRevenue = Table.AddColumn(AddRevenue, "cs_rev", each 
        if [cs_rms] <> null and [cs_adr] <> null then [cs_rms] * [cs_adr] else null, type number),

    AddRevenuePY = Table.AddColumn(AddCSRevenue, "rev_py", each 
        if [rms_py] <> null and [adr_py] <> null then [rms_py] * [adr_py] else null, type number),

    AddCSRevenuePY = Table.AddColumn(AddRevenuePY, "cs_rev_py", each 
        if [cs_rms_py] <> null and [cs_adr_py] <> null then [cs_rms_py] * [cs_adr_py] else null, type number),


    // 📆 Add constant day counts
    AddNoDays = Table.AddColumn(AddCSRevenuePY, "no_days", each 1, type number),
    AddNoDaysPY = Table.AddColumn(AddNoDays, "no_days_py", each 1, type number),

    // 📆 Final enforced typing for Fabric Warehouse safety
    FormattedDates = Table.TransformColumnTypes(
        AddNoDaysPY,
        {
            {"stay_date", type date},
            {"stay_date_py", type date},
            {"stay_date_text", type text},
            {"stay_date_text_py", type text}
        }
    ),
    //-- ✅ Added DateTime Columns to Table
    FinalAddedDatetimeColumns = FormattedDates, 
    //
    //-- ✅ Step 11: Add BigQuery Tracking Columns
    AddedBQColumns = Table.AddColumn(
        Table.AddColumn(FinalAddedDatetimeColumns, "sent_to_big_query", each false, type logical),
        "date_sent_to_big_query", each null, type datetime
    ),
    //
    // 🕒 STEP 20: Add timestamp column
    WithIngestedTimestamp = Table.AddColumn(AddedBQColumns, "ingested_timestamp", each DateTime.LocalNow(), type datetime),
    //
    // Final output and preview options
    RemovedRows = Table.RemoveColumns(WithIngestedTimestamp, { 
    //
         "Source.Name", "compset_lookup", "compset_key", "currency_code",
        
        "occ_%_chg_28d", "occ_%_chg_28d_ly", "occ_%_chg_mtd", "occ_%_chg_mtd_ly", "occ_%_chg_rank_mtd", "occ_%_chg_rank_mtd_ly", "occ_28d", "occ_28d_ly", "occ_index_%_chg_28d", "occ_index_%_chg_28d_ly", "occ_index_%_chg_mtd", "occ_index_%_chg_mtd_ly", "occ_index_28d", "occ_index_28d_ly", "occ_index_mtd", "occ_index_mtd_ly", "occ_mtd", "occ_mtd_ly", "occ_rank_mtd", "occ_rank_mtd_ly", "comp_set_occ_%_chg_28d", "comp_set_occ_%_chg_28d_ly", "comp_set_occ_%_chg_mtd", "comp_set_occ_%_chg_mtd_ly", "comp_set_occ_28d", "comp_set_occ_28d_ly", "comp_set_occ_mtd", "comp_set_occ_mtd_ly", 

        "adr_%_chg_28d", "adr_%_chg_28d_ly", "adr_%_chg_mtd", "adr_%_chg_mtd_ly", "adr_%_chg_rank_mtd", "adr_%_chg_rank_mtd_ly", "adr_28d", "adr_28d_ly", "adr_index_%_chg_28d", "adr_index_%_chg_28d_ly", "adr_index_%_chg_mtd", "adr_index_%_chg_mtd_ly", "adr_index_28d", "adr_index_28d_ly", "adr_index_mtd", "adr_index_mtd_ly", "adr_mtd", "adr_mtd_ly", "adr_rank_mtd", "adr_rank_mtd_ly", "comp_set_adr_%_chg_28d", "comp_set_adr_%_chg_28d_ly", "comp_set_adr_%_chg_mtd", "comp_set_adr_%_chg_mtd_ly", "comp_set_adr_28d", "comp_set_adr_28d_ly", "comp_set_adr_mtd", "comp_set_adr_mtd_ly", 
        
        "revpar_%_chg_28d", "revpar_%_chg_28d_ly", "revpar_%_chg_mtd", "revpar_%_chg_mtd_ly", "revpar_%_chg_rank_mtd", "revpar_%_chg_rank_mtd_ly", "revpar_28d", "revpar_28d_ly", "revpar_index_%_chg_28d", "revpar_index_%_chg_28d_ly", "revpar_index_%_chg_mtd", "revpar_index_%_chg_mtd_ly", "revpar_index_28d", "revpar_index_28d_ly", "revpar_index_mtd", "revpar_index_mtd_ly", "revpar_mtd", "revpar_mtd_ly", "revpar_rank_mtd", "revpar_rank_mtd_ly", "comp_set_revpar_%_chg_28d", "comp_set_revpar_%_chg_28d_ly", "comp_set_revpar_%_chg_mtd", "comp_set_revpar_%_chg_mtd_ly", "comp_set_revpar_28d", "comp_set_revpar_28d_ly", "comp_set_revpar_mtd", "comp_set_revpar_mtd_ly"
    //
    }, MissingField.Ignore),
    //
    ColumnOrder = {
    //
        "property_code", "property_name", "property_shortname", "census_id", "cs_set_id", "cs_reference", "brand",
        "month", "month_name", "cy", "py", "weekday", "weekday_py",  "week_no", "week_no_py", "dow", "day", 
        "stay_date_text", "stay_date_text_py", "stay_date", "stay_date_py", 
        "no_days", "no_days_py", "physical_capacity", "cs_physical_capacity",
        
        "rms", "rms_py", "cs_rms", "cs_rms_py",
        "rev", "rev_py", "cs_rev", "cs_rev_py",

        "occ", "occ_py", "cs_occ", "cs_occ_py", "occ_pct_chg", "occ_pct_chg_py", "cs_occ_pct_chg", "cs_occ_pct_chg_py",
        "occ_index", "occ_index_py",  "occ_index_pct_chg", "occ_index_pct_chg_py",
        "occ_rank", "occ_rank_py", "occ_pct_chg_rank", "occ_pct_chg_rank_py",
        
        "adr", "adr_py", "cs_adr", "cs_adr_py",
        "adr_pct_chg", "adr_pct_chg_py", "cs_adr_pct_chg", "cs_adr_pct_chg_py",
        "adr_index", "adr_index_py", "adr_index_pct_chg", "adr_index_pct_chg_py",
        "adr_rank", "adr_rank_py", "adr_pct_chg_rank", "adr_pct_chg_rank_py",
        
        "revpar", "revpar_py", "cs_revpar", "cs_revpar_py",
        "revpar_pct_chg","revpar_pct_chg_py", "cs_revpar_pct_chg", "cs_revpar_pct_chg_py",
        "revpar_index", "revpar_index_py", "revpar_index_pct_chg", "revpar_index_pct_chg_py",
        "revpar_rank", "revpar_rank_py", "revpar_pct_chg_rank", "revpar_pct_chg_rank_py"
    //
    },
    //
    // STEP 10: Reorder only the columns that exist
    AvailableColumns = Table.ColumnNames(RemovedRows),
    ReorderedColumnsList = List.Intersect({ColumnOrder, AvailableColumns}),
    ReorderedTable = Table.ReorderColumns(RemovedRows, ReorderedColumnsList, MissingField.Ignore),
    //
    // STEP 11: Diagnostics
    ErroredRows = Table.SelectRows(ReorderedTable, each List.ContainsAny(Record.FieldValues(_), {"Error"})),
    //
    // 🚫 STEP 25: Remove unsupported columns before Warehouse write
    RemovedLookupKeys = Table.RemoveColumns(ReorderedTable, { "demand_cs_lookup", "demand_cs_key"}, MissingField.Ignore),
    //
    FinalOutput = RemovedLookupKeys
    //
in
    FinalOutput;
shared CoStar_DayStar_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "6fc7a30e-b793-4185-b9d5-2cbe481efae7"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "f266312a-87c6-47eb-aff9-fd34597688fb"]}[Data],
  TableNavigation = Navigation_2{[Id = "CoStar_DayStar", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
