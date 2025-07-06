[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared vw_tablecolumnmetadata = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "6fc7a30e-b793-4185-b9d5-2cbe481efae7"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "f266312a-87c6-47eb-aff9-fd34597688fb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "vw_tablecolumnmetadata", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared map_schema = let
    Source = Csv.Document(
        Web.Contents("https://docs.google.com/spreadsheets/d/1Aa1MW2arWGBn0uwqHizUt56fhNEtL5opc34gnfKxMHY/export?format=csv&id=1Aa1MW2arWGBn0uwqHizUt56fhNEtL5opc34gnfKxMHY&gid=45129698"),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
in
    PromotedHeaders;
shared Pace_Property = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Pace_Property",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code",
        "dow", "stay_date", "stay_date_py",
        "rms_fct", "rms_fct_py", 
        "rms_fct_trn", "rms_fct_trn_py",
        "rms_fct_grp", "rms_fct_grp_py",
        "adr_fct", "adr_fct_py",
        "rev_fct", "rev_fct_py",
        "revpar_fct", "revpar_fct_py",
        "rms", "rms_py", "rms_stly", "rms_st2y",  "rms_st19",
        "adr","adr_py",
        "rev","rev_py","rev_stly","rev_st2y","rev_st19",
        "revpar", "revpar_py",
        "rms_sold_grp", "rms_sold_grp_py", "rms_sold_grp_stly", "rms_sold_grp_st2y", "rms_sold_grp_st19",
        "rms_sold_trn", "rms_sold_trn_py", "rms_sold_trn_stly", "rms_sold_trn_st2y", "rms_sold_trn_st19",
        "arrivals", "arrivals_py", "departures", "departures_py",
        "rms_ooo", "rms_ooo_py", "rms_other", "rms_other_py",
        "cx", "cx_py", "no_show", "no_show_py",
        "bar", "lrv_py", "lrv",
        "overbooking_py", "overbooking", "wash_pct", "wash_pct_py",
        "special_event", "special_event_py",
        "demand", "demand_py",
        "unconstrained_demand_trn_py", "unconstrained_demand_trn", 
        "constrained_demand_grp_py", "constrained_demand_grp", 
        "projected_rms", "projected_rms_py", "projected_rev", "projected_rev_py",
        "bgt_rms", "bgt_rms_py", "bgt_rev", "bgt_rev_py",
        "remaining_capacity_py", "remaining_capacity", "physical_capacity_py", "physical_capacity",
        "sys_demand_py", "sys_demand", "sys_demand_grp", "sys_demand_trn",
        "sys_demand_trn_py", "sys_demand_grp_py",
        "property_name", "property_shortname",
        "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        timestamp = "TIMESTAMP",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
shared Pace_Segment = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Pace_Segment",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code", "property_name",
        "segment", "dow", "stay_date", "stay_date_py",
        "rms_fct", "rms_fct_py",
        "adr_fct", "adr_fct_py",
        "rev_fct", "rev_fct_py", 
        "rms", "rms_py", "rms_stly", "rms_st2y", "rms_st19",
        "adr", "adr_py",
        "rev", "rev_py", "rev_stly", "rev_st2y", "rev_st19", 
        "arrivals", "arrivals_py", "departures", "departures_py",
        "no_show", "no_show_py", "cx", "cx_py",
        "projected_rms", "projected_rms_py", "projected_rev", "projected_rev_py",
        "bgt_rms", "bgt_rms_py", "bgt_rev", "bgt_rev_py",
        "segment_code", "segment_sort", "segment_group", "segment_group_code", "finance_segment",
        "physical_capacity", "property_shortname",
        "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        timestamp = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
shared Pace_RoomType = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Pace_RoomType",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code", "property_name", "dow", "stay_date", "stay_date_py", 
        "rms_roomtype", "roomtype", 
        "rms", "rms_py", "rms_stly", "rms_st2y", "rms_st19", "rms_other", "rms_other_py", 
        "adr", "adr_py", "adr_fct_py", "adr_fct", 
        "rev", "rev_py", "rev_stly", "rev_st2y", "rev_st19", "rev_fct", "rev_fct_py",
        "revpar", "revpar_py", "revpar_fct", "revpar_fct_py",  
        "arrivals", "arrivals_py", "departures", "departures_py", "no_show", "no_show_py", "rms_ooo", "rms_ooo_py", "cx", "cx_py", 
        "capacity", "capacity_py", "remaining_capacity", "remaining_capacity_py", "overbooking", "overbooking_py", 
        "bar", "bar_py", "physical_capacity", "property_shortname",
        "rms_roomtype", "rms_roomtype_class", "roomtype_code", "roomtype_class", 
        "roomtype_category_code", "roomtype_category_base", "roomtype_category", 
        "roomtype_physical_capacity", "roomtype_sort", 
        "snapshot_date", "ingested_timestamp", 
        "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        timestamp = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
shared Demand_Property = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Demand_Property",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code", "property_name", 
        "stay_date", "segment",
        "occ", "occ_chg_pw", "occ_var_py",  
        "cs_occ", "cs_occ_chg_pw", "cs_occ_var_py",  
        "occ_index", "occ_index_chg_pw", "occ_index_var_py", "occ_rank",
        "adr", "adr_var_py", "cs_adr", "cs_adr_var_py", 
        "adr_rank", "adr_rank_py", "revpar_rank", "revpar_rank_py", 
         "cy", "month_year", "month", "weekday", "dow", 
        "property_shortname", "cs_set_id", "physical_capacity", "cs_physical_capacity", 
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        timestamp = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
shared Demand_Segment = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Demand_Segment",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code", "property_name", 
        "d360_segment",  "segment", "stay_date", 
        "occ", "cs_occ", "occ_index", "occ_rank", 
        "occ_index_var_py", "occ_index_chg_pw", 
        "rms", "rms_pct_var_py", "cs_rms_pct_var_py", 
        "rms_chg_pw", "rms_pct_chg_pw", "cs_rms_pct_chg_pw", 
        "adr", "adr_rank", "revpar", "revpar_rank", 
        "property_shortname",  "cs_set_id", "physical_capacity", "cs_physical_capacity",
        "cy", "month_year", "month", "weekday", "dow", 
        "segment_code",	"segment_sort",	"segment_group", "segment_group_code", "finance_segment",
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        timestamp = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
shared Demand_RoomType = let
    // Load and filter the metadata view
    FullMetadata = vw_tablecolumnmetadata,
    TargetSchema = "dbo",
    TargetTable = "Demand_Channel",
    // Filter the metadata to only include the target table
    FabricTable = Table.SelectRows(FullMetadata, each [TABLE_SCHEMA] = TargetSchema and [TABLE_NAME] = TargetTable),
    // Clean column names to match Table.Schema format
    RenamedFabricTable = Table.RenameColumns(
        FabricTable, {{"COLUMN_NAME", "Name"}, {"DATA_TYPE", "Kind"}, {"ORDINAL_POSITION", "Position"}}
    ),
    //
    CleanSchema = Table.Sort(Table.Distinct(RenamedFabricTable, {"Name"}), {{"Position", Order.Ascending}}),
    // Reorder available columns to match output schema
    ColumnOrder = {
        "property_code", "property_name", 
        "stay_date", "d360_channel",  "channel_group",  "source",
        "occ", "cs_occ", "occ_index", "occ_rank", 
        "occ_index_var_py", "occ_index_chg_pw", 
        "rms", "rms_pct_var_py", "cs_rms_pct_var_py", 
        "rms_chg_pw", "rms_pct_chg_pw", "cs_rms_pct_chg_pw", 
        "adr", "adr_rank", "revpar", "revpar_rank", 
        "cy", "month_year", "month", "weekday", "dow", 
        "channel_group_code",  "source_group_code", "source_group",  "source_code", "channel_sort",
        "property_shortname",  "cs_set_id", "physical_capacity", "cs_physical_capacity", 
        "cs_demand_id", "snapshot_date", "ingested_timestamp", "sent_to_big_query", "date_sent_to_big_query"
    },
    // 🚀 Get only the columns that exist in the current dataset
    AvailableColumns = Table.ColumnNames(CleanSchema),
    FinalColumnOrder = List.Intersect({ColumnOrder, AvailableColumns}),
    //
    // 🚀 Reorder only the available ones, and let the rest stay as-is
    FinalColumnOrderOutput = Table.ReorderColumns(CleanSchema, FinalColumnOrder, MissingField.Ignore),
    //
    // STEP 4: Define type mapping
    TypeMap = [
        text = "STRING",
        varchar = "STRING",
        double = "FLOAT",       
        number = "FLOAT",
        bigint = "INTEGER",
        bit = "BOOLEAN",
        boolean = "BOOLEAN",
        logical = "BOOLEAN",
        datetime = "TIMESTAMP",
        datetime2 = "TIMESTAMP",
        timestamp = "TIMESTAMP",
        date = "DATE",
        time = "TIME",
        duration = "STRING",
        binary = "BYTES",
        float = "FLOAT"
    ],
    //
    // Map schema join (optional external mapping for descriptions)
    MapSchemaData = try Table.RowCount(map_schema) > 0 otherwise false,
    MergedSchemaData =
        if MapSchemaData then
            Table.NestedJoin(
                FinalColumnOrderOutput, {"Name"}, map_schema, {"table_name"}, "map_schema", JoinKind.LeftOuter
            )
        else
            FinalColumnOrderOutput,
    ExpandedSchemaData =
        if MapSchemaData then
            Table.Distinct(Table.ExpandTableColumn(MergedSchemaData, "map_schema", {"definition"}), {"Name"})
        else
            FinalColumnOrderOutput,
    AddSortIndex = Table.AddColumn(
        ExpandedSchemaData, "SortIndex", each List.PositionOf(ColumnOrder, [Name]), Int64.Type
    ),
    SortedSchema = Table.Sort(AddSortIndex, {{"SortIndex", Order.Ascending}}),
    ReorderedSchemaData = Table.RemoveColumns(SortedSchema, {"SortIndex"}),
    //
    // Add BigQuery field record
    BQSchema = Table.AddColumn(
        ReorderedSchemaData,
        "BQ_Field",
        each
            [
                name = [Name],
                mode = "NULLABLE",
                type = Record.FieldOrDefault(TypeMap, Text.Lower([Kind]), "STRING"),
                description = try [definition] otherwise "",
                fields = {}
            ],
        type record
    ),
    //
    // Format to JSON output
    BQFieldList = Table.Column(BQSchema, "BQ_Field"),
    ColumnBlock = "[" & Text.Combine(
        List.Transform(BQFieldList, each 
            "{" &
            """" & "name" & """" & ": " & """" & _[name] & """" & ", " &
            """" & "mode" & """" & ": " & """" & _[mode] & """" & ", " &
            """" & "type" & """" & ": " & """" & _[type] & """" & ", " &
            """" & "description" & """" & ": " & """" & Text.Replace(_[description], """", "'") & """" & ", " &
            """" & "fields" & """" & ": []" &
            "}"
        ),
        "," & "#(cr)#(lf)"
    ) & "]"
in
    ColumnBlock;
