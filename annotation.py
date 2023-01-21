import random
import re
from queue import Queue


QEP_DATA_TYPE_SPECIFIER_REGEX = "::[^\(\)\,]*"

RELATION_ATTRIBUTE_REGEX = "(?<!'|\")[a-zA-Z][\w.]*(?!'|\")"

JOIN_CONDITION_REGEX = "[\w.]+ = [\w.]+"

join_condition_queue = Queue()


def remove_type_specifier(input_string):
    if "numeric" in input_string:
        input_string = input_string.replace("'", "")
    return re.sub(QEP_DATA_TYPE_SPECIFIER_REGEX, '', input_string)


def add_relation_names(filters_string, table_name):
    attributes_list = re.findall(RELATION_ATTRIBUTE_REGEX, filters_string)
    tablename_attributes_list = [
        attribute if "." in attribute else table_name + "." + attribute for attribute in attributes_list]

    result = filters_string
    for i in range(len(attributes_list)):
        result = result.replace(
            attributes_list[i], tablename_attributes_list[i])

    return result


def filter_splitter(filters_string):
    # Filters/conditions are in the format "(condition1), (condition2), ..."
    DELIMITER = ","
    filters_list = filters_string.split(DELIMITER)
    filters_list = [filter_string.strip()[1:-1]
                    for filter_string in filters_list]

    return filters_list


def find_join_conditions(filters_string):
    join_conditions_list = re.findall(JOIN_CONDITION_REGEX, filters_string)

    for join_condition in join_conditions_list:
        join_condition_queue.put(join_condition)


def get_join_condition():
    return join_condition_queue.get()


def get_table_names(qep):
    table_names = []
    if "Plans" in qep:
        for plan in qep["Plans"]:
            table_names += get_table_names(plan)
    elif "Alias" in qep:
        table_names.append(qep["Alias"])
    elif "Relation Name" in qep:
        table_names.append(qep["Relation Name"])
    return table_names


##################################################################################################
################################Scan methods######################################################
##################################################################################################


def seq_scan(qep):

    annotation = "A sequential scan is done on the relation "
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Relation Name" in qep):
        table_name = qep['Relation Name']
        annotation += f"{table_name}"

    if ("Alias" in qep):
        table_name = qep['Alias']
        annotation += ", that has an alias name: "
        annotation += f"{table_name}. "
    else:
        annotation += ". "

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])
        filter_string = add_relation_names(filter_string, table_name)

        find_join_conditions(filter_string)

        annotation += f"It is also filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    annotation += "\n"
    return [(annotation, filters)]


def index_scan(qep):

    annotation = f"An index scan is done using the index {qep['Index Name']} on it's index table"

    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Alias" in qep):
        table_name = qep['Alias']
    elif ("Relation Name" in qep):
        table_name = qep['Relation Name']

    if ("Index Cond" in qep):
        condition_string = remove_type_specifier(qep['Index Cond'])
        condition_string = add_relation_names(condition_string, table_name)

        find_join_conditions(condition_string)

        annotation += f", with the following condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)
    else:
        annotation += ". "

    annotation += f"This scan is chosen as there is an index on {qep['Index Name']}. "

    if ("Node Type" in qep == "Index Scan"):
        annotation += f"Then, it accesses the relation {qep['Relation Name']} table and retrieves rows matching with the index {qep['Index Name']}. "

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])
        filter_string = add_relation_names(filter_string, table_name)

        find_join_conditions(filter_string)

        annotation += f"It is also filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    annotation += "\n"
    return [(annotation, filters)]


def index_only_scan(qep):

    annotation = f"An index scan is done using the index {qep['Index Name']} on it's index table"

    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Alias" in qep):
        table_name = qep['Alias']
    elif ("Relation Name" in qep):
        table_name = qep['Relation Name']

    if ("Index Cond" in qep):
        condition_string = remove_type_specifier(qep['Index Cond'])
        condition_string = add_relation_names(condition_string, table_name)

        find_join_conditions(condition_string)

        annotation += f", with the following condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)
    else:
        annotation += ". "

    annotation += f"This scan is chosen as there is an index on {qep['Index Name']}. "

    if ("Node Type" in qep == "Index Only Scan"):
        annotation += f"Then, it retrieves rows matching with the index {qep['Index Name']}. "

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])
        filter_string = add_relation_names(filter_string, table_name)

        find_join_conditions(filter_string)

        annotation += f"It is also filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    annotation += "\n"
    return [(annotation, filters)]


def cte_scan(qep):

    annotation = "A CTE scan is done sequentially on the materialized results "
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Alias" in qep):
        table_name = qep['Alias']
    elif ("Relation Name" in qep):
        table_name = qep['Relation Name']

    annotation += f"named {qep['CTE Name']}"

    if ("Index Cond" in qep):
        condition_string = remove_type_specifier(qep['Index Cond'])
        condition_string = add_relation_names(condition_string, table_name)

        find_join_conditions(condition_string)

        annotation += f", with the following condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)
    else:
        annotation += ". "

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])
        filter_string = add_relation_names(filter_string, table_name)

        find_join_conditions(filter_string)

        annotation += f"It is also filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    annotation += "\n"
    return [(annotation, filters)]


def bitmap_index_scan(qep):

    annotation = f"An index scan is done using the index {qep['Index Name']} on it's index table to create a bitmap of satisfactory pages"
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Index Cond" in qep):
        condition_string = remove_type_specifier(qep['Index Cond'])

        annotation += f", with the following condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)
    else:
        annotation += ". "

    annotation += "\n"
    return [(annotation, filters)]


def bitmap_heap_scan(qep):

    annotation = "A bitmap heap scan is done using the bitmap created from bitmap index scan. "
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Alias" in qep):
        table_name = qep['Alias']
    elif ("Relation Name" in qep):
        table_name = qep['Relation Name']

    if ("Recheck Cond" in qep):
        condition_string = remove_type_specifier(qep['Index Cond'])
        condition_string = add_relation_names(condition_string, table_name)

        find_join_conditions(condition_string)

        annotation += f"Results are filtered with the following recheck condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])
        filter_string = add_relation_names(filter_string, table_name)

        find_join_conditions(filter_string)

        annotation += f"It is also filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    annotation += "\n"
    return [(annotation, filters)]


##################################################################################################
################################Join methods######################################################
##################################################################################################

JOIN_NODE_TYPES = ("Nested Loop", "Hash Join", "Merge Join")


def nested_loop_join(qep):

    annotation = "A nested-loop join is done using the two relations. "
    annotation += "This is chosen as the relation size involved is smaller. "
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Join Filter" in qep):
        join_filter_string = remove_type_specifier(qep['Join Filter'])

        annotation += f"Before the join, filter with the following condition(s) is applied: {join_filter_string}. "
        filters["cond"] += filter_splitter(join_filter_string)

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])

        annotation += f"Join result is filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    if not filters["cond"]:
        bubbled_filter = get_join_condition()
        filters["cond"].append(bubbled_filter)

    annotation += "\n"
    return [(annotation, filters)]


def hash_join(qep):

    annotation = "A hash join is done using the two relations. "
    annotation += "This is chosen as no indexes exist and the relations are not sorted. "
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Hash Cond" in qep):
        condition_string = remove_type_specifier(qep['Hash Cond'])

        annotation += f"The join is done with the following condition(s): {condition_string}. "

        filters["cond"] += filter_splitter(condition_string)

    if ("Join Filter" in qep):
        join_filter_string = remove_type_specifier(qep['Join Filter'])

        annotation += f"Before the join, filter with the following condition(s) is applied: {join_filter_string}. "
        filters["cond"] += filter_splitter(join_filter_string)

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])

        annotation += f"Join result is filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    if not filters["cond"]:
        bubbled_filter = get_join_condition()
        filters["cond"].append(bubbled_filter)

    annotation += "\n"
    return [(annotation, filters)]


def merge_join(qep):

    annotation = "A merge join is done using the two relations"
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ("Merge Cond" in qep):
        condition_string = remove_type_specifier(qep['Hash Cond'])

        annotation += f", with the following condition(s): {condition_string}. "
        filters["cond"] += filter_splitter(condition_string)
    else:
        annotation += ". "

    if ("Join Filter" in qep):
        join_filter_string = remove_type_specifier(qep['Join Filter'])

        annotation += f"Before the join, filter with the following condition(s) is applied: {join_filter_string}. "
        filters["cond"] += filter_splitter(join_filter_string)

    if ("Filter" in qep):
        filter_string = remove_type_specifier(qep['Filter'])

        annotation += f"Join result is filtered with the following condition(s): {filter_string}. "
        filters["cond"] += filter_splitter(filter_string)

    if not filters["cond"]:
        bubbled_filter = get_join_condition()
        filters["cond"].append(bubbled_filter)

    annotation += "\n"
    return [(annotation, filters)]


JoinTypeMap = {
    "Hash Join": hash_join,
    "Merge Join": merge_join,
    "Nested Loop": nested_loop_join
}


def join(qep):

    result = []

    outer_relation_index = 0 if (
        qep["Plans"][0]["Parent Relationship"] == "Outer") else 1
    inner_relation_index = 1 - outer_relation_index

    # Process left child (outer relation)
    outer_relation_annotation = annotate(
        qep["Plans"][outer_relation_index])

    # Process right child (inner relation)
    inner_relation_annotation = annotate(
        qep["Plans"][inner_relation_index])

    current_join = JoinTypeMap[qep["Node Type"]]
    current_join_annotation = current_join(qep)

    result += outer_relation_annotation
    result += inner_relation_annotation
    result += current_join_annotation

    return result

##################################################################################################
################################Utility methods###################################################
##################################################################################################


def hash(qep):

    result = []
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    scan_annotation = annotate(qep['Plans'][0])

    hash_annotation = f"A hash is performed on the \"{qep['Plans'][0]['Relation Name']}\" relation"

    if ("Alias" in qep):
        table_name = qep['Alias']
        hash_annotation += ", that has an alias name: "
        hash_annotation += f"{table_name}. "
    else:
        hash_annotation += ". "

    hash_annotation += "\n"

    result += scan_annotation
    result.append((hash_annotation, filters))

    return result


def aggregate(qep):

    annotation = ''
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if (qep['Strategy'] == 'Hashed'):

        length = len(qep["Group Key"])

        if (length > 1):
            annotation += "The results are produced after hashing on the following keys: "
            for key in qep["Group Key"]:
                key_string = remove_type_specifier(qep['Group Key'])
                annotation += key_string + ", "
                filters["cond"] += filter_splitter(key_string)

        else:
            annotation += "The results are produced after hashing on the single key: "
            key_string = remove_type_specifier(qep['Group Key'][0])
            annotation += key_string
            filters["cond"] += filter_splitter(key_string)

        annotation += '.\n'

        return annotate(qep['Plans'][0]) + [(annotation, filters)]

    if (qep['Strategy'] == "Plain"):

        annotation += annotate(qep['Plans'][0])
        annotation += "The results are aggregated.\n"

        return [(annotation, filters)]

    if (qep['Strategy'] == 'Sorted'):

        annotation += annotate(qep['Plans'][0])

        if ("Group Key" in qep):

            annotation += "It is grouped by the following keys: "

            for key in qep['Group Key']:

                key_string = remove_type_specifier(qep['Group Key'])
                annotation += key_string + ", "
                filters["cond"] += filter_splitter(key_string)

            annotation = annotation[:-2] + "."

        if ("Filter" in qep):

            filter_string = remove_type_specifier(qep['Filter'])

            annotation += f"It is also filtered with the following condition(s): {filter_string}."
            filters["cond"] += filter_splitter(filter_string)

        annotation += "\n"
        return [(annotation, filters)]


def unique(qep):

    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    annotation = annotate(qep['Plans'][0])
    annotation += "Also, only unique values of the data is being kept."

    annotation += '\n'
    return [(annotation, filters)]


def groupby(qep):

    annotation = ''
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    annotation += annotate(qep['Plans'][0])
    length = len(qep['Group Key'])

    if (length > 1):
        annotation += "The results are grouped by the following keys: "
        for key in qep["Group Key"]:
            key_string = remove_type_specifier(qep['Group Key'])
            annotation += key_string + ", "
            filters["cond"] += filter_splitter(key_string)

    else:
        key_string = remove_type_specifier(qep['Group Key'][0])
        annotation += key_string
        filters["cond"] += filter_splitter(key_string)

    annotation += '.\n'
    return [(annotation, filters)]


def limit(qep):

    filters = {
        "table": get_table_names(qep),
        "cond": []
    }
    annotation = annotate(qep['Plans'][0])
    annotation += 'The results are limited by '
    number_string = remove_type_specifier(qep['Plan Rows'])
    annotation += str(qep['Plan Rows']) + " rows of data entry."
    filters["cond"] += filter_splitter(number_string)

    annotation += '\n'
    return [(annotation, filters)]


def sort(qep):

    annotation = ''
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    if ('Plans' in qep):
        for x in qep['Plans']:
            annotation += ' ' + annotate(x)

    if (qep["Node Type"] == 'Sort'):

        length = len(qep['Sort Key'])

        while (length >= 1):

            key = str(qep['Sort Key'][length-1])
            key_string = remove_type_specifier(qep['Sort Key'][length-1])
            filters["cond"] += filter_splitter(key_string)

            if ('DESC' in key):
                annotation += "The results are sorted in descending order using the key(s), "
                annotation += key.replace('DESC', '.')

            else:
                annotation += "The results are sorted in ascending order using the key(s), "
                annotation += key + '.'

            length = length - 1

    annotation += '\n'
    return [(annotation, filters)]


def append(qep):
    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    annotation = ''

    if ('Plans' in qep):
        for x in qep['Plans']:
            annotation += ' ' + annotate(x)

    if (qep["Node Type"] == "Append"):

        annotation += "The results are being appended together."

    annotation += '\n'
    return [(annotation, filters)]


def notFound(qep):

    filters = {
        "table": get_table_names(qep),
        "cond": []
    }

    annotation = f"{qep['Node Type']} is performed."
    if ('Plans' in qep):
        for x in qep['Plans']:
            annotation += ' ' + annotate(x)

    annotation += "\n"
    return [(annotation, filters)]


##################################################################################################
################################Main method call##################################################
##################################################################################################

# Maps Node Types to functions
NodeTypeMap = {
    "Seq Scan": seq_scan,
    "Index Scan": index_scan,
    "Index Only Scan": index_only_scan,
    "CTE Scan": cte_scan,
    "Bitmap Heap Scan": bitmap_heap_scan,
    "Bitmap Index Scan": bitmap_index_scan,
    "Hash Join": join,
    "Merge Join": join,
    "Nested Loop": join,
    "Hash": hash,
    "Aggregate": aggregate,
    "Sort": sort,
    "Limit": limit,
    "Group": groupby,
    "Unique": unique,
    "Append": append,
    "NotFound": notFound
}


def annotate(qep):
    try:
        operation = NodeTypeMap.get(qep["Node Type"])
    except:
        operation = NodeTypeMap.get('NotFound')

    return operation(qep)
