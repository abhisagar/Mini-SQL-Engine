import os, sys, re
import itertools

query_str = ""
tables = {}
query_columns = []
condOp = None
aggregationOp = None
distinctOp = None
query_tables = []
query_conditions = []
query_data = {"columns": [], "data": {}}
projected_data = {"columns": [], "data": {}}
operators = {
    "AND": lambda a, b: [val for val in a if val in b],
    "<=": lambda a, b: a <= b,
    "<": lambda a, b: a < b,
    "OR": lambda a, b: a + b,
    ">=": lambda a, b: a >= b,
    ">": lambda a, b: a > b,
    "=": lambda a, b: a == b,
}

DATA_FOLDER = "files"


def execute_query():
    global query_str
    query_str = query_str.strip(" ")
    read_metadata()
    parse_query()
    run_query()


def correctFormat(query):
    if query[-1] != ';':
        sys.exit("You are missing a semicolon")
    return bool(re.match('^select.*from.*', query))


def parse_query():
    global query_str, query_columns, query_tables, query_conditions
    if not correctFormat(query_str):
        sys.exit("Incorrect Sql Query")
    query_str = query_str[:-1]
    tokens = query_str.lower().split(" ")
    case_tokens = query_str.split(" ")
    handle_error(tokens[0] == "select" and tokens.count("select") == 1)
    handle_error("from" in tokens and tokens.count("from") == 1)
    query_columns = parse_columns(
        " ".join(case_tokens[1: tokens.index("from")])
    )
    if "where" in tokens:
        handle_error(
            tokens.index("where") != len(tokens) - 1,
            "No condition provided after where",
        )
        handle_error(
            tokens.index("where") - tokens.index("from") > 1,
            "No table names provided",
        )
        query_tables = parse_tables(
            " ".join(case_tokens[tokens.index("from") + 1: tokens.index("where")])
        )
        query_conditions = parse_conditions(
            "".join(case_tokens[tokens.index("where") + 1:])
        )
    else:
        handle_error(
            tokens.index("from") != len(tokens) - 1, "No table names provided"
        )
        query_tables = parse_tables(
            " ".join(case_tokens[tokens.index("from") + 1:])
        )


def parse_columns(colstr):
    global distinctOp, aggregationOp
    colstr = colstr.strip(" ")
    if re.match("^distinct", colstr):
        colstr = colstr[8:]
        distinctOp = True
    columns = colstr.split(",")
    temp_columns = []
    for col in columns:
        if col != " " or col != "":
            temp_columns.append(col)
    columns = temp_columns
    if len(columns) > 1:
        handle_error(
            all(re.match("^[\w*.-]+$", s) is not None for s in columns)
        )
    else:
        if re.match("^(sum|max|avg|min)(\([\w*.-]+\))$", columns[0]):
            aggregationOp = columns[0][:3]
            return [columns[0][4:-1]]
        else:
            handle_error(re.match("^[\w*.-]+$", columns[0]) is not None)
    return columns


def parse_tables(tablestr):
    global tables
    query_tables = tablestr.strip(" ").split(",")
    query_tables = [table.strip(" ") for table in query_tables if table != " " or table != ""]
    handle_error(all(re.match("^[\w-]+$", s) is not None for s in query_tables))
    for table in query_tables:
        if table not in tables:
            sys.exit("Mentioned Table '" + table + "' not found")
    return query_tables


def parse_conditions(condstr):
    global condOp, operators
    condstr, ret = condstr.strip(" "), []
    search = re.search("AND|OR", condstr)
    if search:
        condOp = search.group()
        condstr = re.sub("AND|OR", " ", condstr)
        conditions = re.split("\s+", condstr)
    else:
        conditions = [condstr]
    for cond in conditions:
        operator = None
        for op in ["<=", ">=", ">", "<", "="]:
            if re.search(op, cond):
                operator = re.search(op, cond).group()
                break
        handle_error(operator)
        parts = re.split("\s+", re.sub("<|>|<=|>=|=", " ", cond))
        ret.append((parts[0], operator, parts[1]))

    return ret


def run_query():
    join_tables()
    execute_conditions()
    if aggregationOp is not None:
        execute_aggregation(query_columns[0])
    else:
        project_columns()
        display_table()


def join_tables():
    columns = []
    for table in query_tables:
        temp = []
        for col in tables[table]["columns"]:
            query_data["columns"].append(table + "." + col)
            query_data["data"][table + "." + col] = []
            temp.append(tables[table]["data"][col])
        colum = list(zip(*temp))
        columns.append(colum)
    joined_data = list(itertools.product(*columns))
    for obj in joined_data:
        row = []
        for data in obj:
            row.extend(list(data))
        for i, col in enumerate(query_data["columns"]):
            query_data["data"][col].append(row[i])


def check_column(column):
    if "." in column:
        if column not in query_data["columns"]:
            sys.exit("Wrong field provided")
        col = column
    else:
        data_columns = [col.split(".")[1] for col in query_data["columns"]]
        if column not in data_columns:
            sys.exit("Wrong field provided")
        handle_error(data_columns.count(column) == 1, "Ambiguous column name given")
        col = query_data["columns"][data_columns.index(column)]
    return col


def get_matching_indices(cond):
    ret = []
    colname = check_column(cond[0])
    col1 = query_data["data"][colname]
    if not is_int(cond[2]):
        colname = check_column(cond[2])
        col2 = query_data["data"][colname]
        for i, val in enumerate(zip(col1, col2)):
            if operators[cond[1]](val[0], val[1]) == True:
                ret.append(i)
    else:
        for i, val in enumerate(col1):
            if operators[cond[1]](val, int(cond[2])):
                ret.append(i)
    return ret


def execute_conditions():
    if query_conditions:
        filteredInd = []
        ind1 = get_matching_indices(query_conditions[0])
        if condOp:
            ind2 = get_matching_indices(query_conditions[1])
            filteredInd = operators[condOp](ind1, ind2)
        else:
            filteredInd = ind1
        for col in query_data["columns"]:
            query_data["data"][col] = [query_data["data"][col][i] for i in filteredInd]


def execute_aggregation(col):
    global query_data
    col = check_column(col)
    data = query_data["data"][col]
    if aggregationOp == "max":
        ret = max(data)
    elif aggregationOp == "sum":
        ret = sum(data)
    elif aggregationOp == "min":
        ret = min(data)
    else:
        ret = sum(data) / len(data)
    print(aggregationOp + "(" + col + ")")
    print(ret)


def project_columns():
    global projected_data
    if query_columns[0] != "*":
        for col in query_columns:
            column_name = check_column(col)
            projected_data["columns"].append(column_name)
            projected_data["data"][column_name] = query_data["data"][column_name]
    else:
        projected_data = query_data


def display_table():
    global projected_data
    header, data, rows = [], [], []
    for col in projected_data["columns"]:
        header.append(col)
        data.append(projected_data["data"][col])
    print(",".join(header))
    for i in range(len(data[0])):
        row = []
        for j in range(len(header)):
            row.append(str(data[j][i]))
        if distinctOp:
            if row in rows:
                continue
            else:
                print(",".join(row))
                rows.append(row)
        else:
            print(",".join(row))
            rows.append(row)


def read_table(table_metadata):
    tablename = table_metadata[0]
    columns = table_metadata[1:]
    tables[tablename] = {"columns": columns, "data": {}}
    tables[tablename]["data"] = dict(zip(columns, [[] for i in range(len(columns))]))
    file_path = os.path.join(DATA_FOLDER, tablename + '.csv')
    with open(file_path) as f:
        for line in f.readlines():
            line = line.strip("\n")
            values = line.split(",")
            for ind, col in enumerate(columns):
                col_value = re.sub("['\"]", "", values[ind])
                tables[tablename]["data"][col].append(int(col_value))


def read_metadata():
    metadata, file_path = [], os.path.join(DATA_FOLDER, "metadata.txt")
    with open(file_path) as f:
        for line in f.readlines():
            metadata.append(line.strip("\n"))

    start_index, end_index = -1, -1
    for i in range(len(metadata)):
        if metadata[i] == "<begin_table>":
            start_index = i
        elif metadata[i] == "<end_table>":
            end_index = i
        if start_index != -1 and end_index != -1:
            read_table(metadata[start_index + 1: end_index])
            start_index, end_index = -1, -1


def handle_error(cond=False, msg="Incorrect query format"):
    if not cond:
        sys.exit(msg)

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Query string not provided")
    else:
        query_str = sys.argv[1]
        execute_query()
