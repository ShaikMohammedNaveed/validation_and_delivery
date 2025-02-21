import os
import json
import yaml
import re
import shutil
from collections import Counter
from collections import defaultdict
import pandas as pd
import gspread
from collections import defaultdict, Counter
from oauth2client.service_account import ServiceAccountCredentials
# Other constants
GOOGLE_API_CREDENTIALS_PATH = os.path.join(os.getcwd(), "common/credentials.json")

all_instances = []
errors_task_id = []
error_cell = []
error_msg = []
collab_error_links = []
error_truns = []
error_message = []

MAPPING = {
    "Assistant - Code": "code",
    "Assistant - Scaffolding code": "scaffolding_code",
    "Assistant - Blueprint": "blueprint",
    "Assistant - Implementation plan": "implementation_plan",
    "Assistant - Clarification Question": "clarification_question",
    # None: 'others'
}


MAP_renaming = {
    "Content Requirement": "contentRequirements",
}

regex_scf = (
    r"[\w]+(?:\.[\w]+)?\.html\.scaf|"
    r"[\w]+(?:\.[\w]+)?\.js\.scaf|"
    r"[\w]+(?:\.[\w]+)?\.cls\.scaf|"
    r"[\w]+(?:\.[\w]+)?\.css\.scaf|"
    r"[\w]+(?:\.[\w]+)?\.apex\.scaf"
)

regex_code = (
    r"[\w]+(?:\.[\w]+)?\.html`|"
    r"[\w]+(?:\.[\w]+)?\.js`|"
    r"[\w]+(?:\.[\w]+)?\.cls`|"
    r"[\w]+(?:\.[\w]+)?\.css`"
)



def extract_collab_id(colab_link):
    """
    Extracts the Collab ID from a given Colab link.

    Args:
        colab_link (str): The full Google Colab URL.
        
    Returns:
        str: The extracted Collab ID.
    """
    match = re.search(r"/drive/([\w-]+)", colab_link)
    return match.group(1) if match else colab_link

def copy_missing_jsons(source_folder, destination_folder, df, colab_column):
    """
    Copies all JSON files **not listed** in the DataFrame to another folder.

    Args:
        source_folder (str): Path to the folder containing JSON files.
        destination_folder (str): Path to the folder where missing JSONs will be copied.
        df (pd.DataFrame): DataFrame containing a list of Colab links.
        colab_column (str): Column name that contains Colab links.

    Returns:
        dict: Summary with counts of copied files and any errors.
    """
    os.makedirs(destination_folder, exist_ok=True)  # Create the destination folder if it doesn't exist

    # Extract valid JSON file names from the DataFrame
    expected_json_files = set(df[colab_column].apply(extract_collab_id) + ".json")

    # List all JSON files in the source folder
    all_json_files = {f for f in os.listdir(source_folder) if f.endswith(".json")}

    # Find files NOT in the DataFrame
    missing_jsons = all_json_files - expected_json_files

    copied_count = 0
    errors = []

    for json_file in missing_jsons:
        src_path = os.path.join(source_folder, json_file)
        dest_path = os.path.join(destination_folder, json_file)

        try:
            shutil.copy(src_path, dest_path)  # Copy the file
            copied_count += 1
        except Exception as e:
            errors.append(f"Error copying {json_file}: {str(e)}")

    return {
        "total_files": len(all_json_files),
        "copied_files": copied_count,
        "errors": errors
    }



def match_code_block(text, idx, cell_id):

    # Regex pattern to match any content within triple backticks ignoring language tag
    pattern = r"```(?:\w+)?\s*([\s\S]+?)\s*```"

    # Use re.search to find the match and capture the content
    match = re.search(pattern, text)

    # Extract and print the captured content if a match is found
    if match:
        content = match.group(
            1
        ).strip()  # Use strip() to remove any extraneous whitespace
    else:
        # print("No content found within triple backtick blocks")
        # print(cell_id)
        # print(idx)
        errors_task_id.append(idx)
        error_cell.append(cell_id)
        error_msg.append("No content found within triple backtick blocks   " + text)
        return ""
    return content


def extract_data(input_str: str, regex_to_use, original_uri, cell_id):

    lines = input_str.splitlines()

    # Initialize variables to store blocks
    blocks = []
    current_block = defaultdict(list)
    
    for line in lines:
        # Check if the line matches the filename pattern
        if re.search(regex_to_use, line.strip()):
            # If there's a current block, add it to blocks
            if current_block:
                if "file_name" in current_block:
                    blocks.append(current_block)

            # Start a new block with the matched line
            current_block = defaultdict(list)
            current_block["file_name"].append(line)
        else:
            # If the current line doesn't start a new block, add it to the current block
            current_block["content"].append(line)

    # Don't forget to add the last collected block
    if current_block:
        if "file_name" in current_block:
            blocks.append(current_block)

    # filter
    rt = {"js": [], "html": [], "css": []}
    for block in blocks:

        head = block["file_name"][0].lower()
        if ".html" in head:
            lange = "html"
        elif ".js" in head or ".javascript" in head:
            lange = "js"
        elif ".css" in head:
            lange = "css"
        else:
            continue
        code_block = "\n".join(block["content"])
        res = match_code_block(code_block, original_uri, cell_id)
        rt[lange].append(res)
    return rt



def to_camel_case(s):
    if s in MAP_renaming:
        return MAP_renaming[s]
    # Remove underscores and hyphens
    s = re.sub(r"(_|-)+", " ", s)

    # Split by spaces and capitalize words
    parts = s.split()
    camel_case = parts[0].lower() + "".join(word.capitalize() for word in parts[1:])

    return camel_case


def convert_to_text(data):
    dict_rename_with_camel = {}
    for section, content_list in data.items():
        camel_section = to_camel_case(section)
        contents = []
        for content in content_list:
            new_dict_content = {}
            for key, value in content.items():
                camel_key = to_camel_case(key)
                new_dict_content[camel_key] = value
            contents.append(new_dict_content)
        dict_rename_with_camel[camel_section] = contents

    yaml_data = yaml.dump(dict_rename_with_camel, default_flow_style=False)
    return yaml_data

def parse_code_blocks(input_string):
    """
    Parse the input string and extract code blocks, mapping them by type to standardized names with lists of strings.

    Args:
        input_string (str): The input string containing code blocks.

    Returns:
        dict: A dictionary mapping standardized code block types (e.g., 'html', 'js', 'css', etc.) to lists of their content.
    """
    # Define the mapping for standardizing code block types
    code_block_type_map = {
        "css": "css",
        "apex": "apex",
        "html": "html",
        "Html": "html",
        "javascript": "js",
        "json": "json",
        "java": "java",
        "js": "js",
        "xml": "xml",
        "Apex": "apex",
    }
    
    # Regex to match code blocks
    pattern = r"`[^`]+?\.(\w+)\.scaf`\s*```(\w+)\n(.*?)```"
    
    # Find all matches
    matches = re.findall(pattern, input_string, re.DOTALL)
    
    # Build the dictionary
    code_blocks = {}
    for block_type, lang, content in matches:
        # Standardize the language using the mapping
        standardized_lang = code_block_type_map.get(lang, lang)  # Default to original if not in map
        if standardized_lang not in code_blocks:
            code_blocks[standardized_lang] = []
        # Append the content to the appropriate list
        code_blocks[standardized_lang].append(content.strip())
    
    return code_blocks


# def main_validator(path):
#     files = os.listdir(path)
#     all_lines = []
#     all_metadata = []
#     for f in files:
#         with open(f'{path}/{f}', 'r') as file:
#             data = json.load(file)
#             all_lines.append(data['data']['messages'])
#             all_metadata.append(data['metadata'])
    
#     cnt_turns = Counter()
#     for idx, l in enumerate(all_lines):
#         #print(files[idx])
#         ex = []
#         buffer = []
#         metadata = all_metadata[idx]["data"]["original_uri"]
#         for cell_id, element in enumerate(l):
#             role = element["role"]
#             if role == "User" and buffer:
#                 ex.append(buffer)
#                 buffer = []  # every time we see a user, we start a new turn

#             content = element["content"]
#             if type(content) == dict:
#                 content = convert_to_text(content)
#             elif isinstance(content, str):
#                 pass
#             else:
#                 raise ValueError(f"Unexpected content type: {type(content)}")

#             if role == "User":
#                 tag = "user_query"
#                 buffer.append(
#                     {"role": "user", "content": content, "tag": tag, "metadata": metadata}
#                 )
#             else:
#                 try:
#                     tag = MAPPING[role]
#                     if tag in ["code"]:
#                         # process code
#                         processed_content = extract_data(content, regex_code, all_metadata[idx]['data']['original_uri'],cell_id)
#                         buffer.append(
#                             {
#                                 "role": "assistant",
#                                 "content": processed_content,
#                                 "content_raw": content,
#                                 "tag": tag,
#                                 "metadata": metadata,
#                             }
#                         )
#                     elif tag in ["scaffolding_code"]:
#                         # process scaffolding code
#                         processed_content = extract_data(content, regex_scf,all_metadata[idx]['data']['original_uri'],cell_id)
#                         buffer.append(
#                             {
#                                 "role": "assistant",
#                                 "content": processed_content,
#                                 "content_raw": content,
#                                 "tag": tag,
#                                 "metadata": metadata,
#                             }
#                         )
#                     else:
#                         buffer.append(
#                             {
#                                 "role": "assistant",
#                                 "content": content,
#                                 "tag": tag,
#                                 "metadata": metadata,
#                             }
#                         )
#                 except KeyError:
#                     print(
#                         f"Role/intention error: {role} from {all_metadata[idx]['data']}\t\t{content[:2000]}"
#                     )
#                     errors_task_id.append(all_metadata[idx]['data']['original_uri'])
#                     error_cell.append(cell_id)
#                     error_msg.append(f"Role/intention error: {role} from {all_metadata[idx]['data']}\t\t{content[:2000]}")

#         if buffer:
#             ex.append(buffer)
#         cnt_turns[len(ex)] += 1

#         all_instances.append(ex)

#     cnt = 0
#     total = 0
#     good_instances = []
#     collab_error_links = []
#     error_truns = []
#     error_message = []
#     for instance in all_instances:
#         lengths = [len(x) for x in instance if len(x) != 2 and len(x) != 5]
#         # check if the first round is user query
#         if instance[0][0]['role'] != 'user':
#             print(f"First round role is not user: {instance[0][0]['role']} {instance[0][0]['metadata']} Content: {instance[0][0]['content'][:20]}")
#             err_msg = f"First round role is not user: {instance[0][0]['role']} Content: {instance[0][0]['content'][:20]}"
#             turn_count = 1
#             collab_error_links.append(instance[0][0]['metadata'])
#             error_truns.append(turn_count)
#             error_message.append(err_msg) 
#             continue
#         if not lengths:
#             good_instances.append(instance)
#             continue
#         for idx, turns in enumerate(instance):
#             total += 1
#             if len(turns) == 2 or len(turns) == 5:
#                 continue
#             cnt += 1
#             print(f"Number of turns error. Expected 2 or 5, got {len(turns)} in turn {idx}. { {instance[0][0]['metadata']} }")
#             turn_count=idx
#             err_msg=f"Number of turns error. Expected 2 or 5, got {len(turns)} in turn {idx}."
#         collab_error_links.append(instance[0][0]['metadata'])
#         error_truns.append(turn_count)
#         error_message.append(err_msg)    
#     content_error_df = pd.DataFrame({"id": errors_task_id, "error_cell": error_cell, "error_msg": error_msg})
#     structure_error_df = pd.DataFrame({"id": collab_error_links, "error_trun": error_truns, "error_msg": error_message})
#     print(content_error_df)
#     print(structure_error_df)




def create_google_sheet(sheet_name, content_df, structure_df,path_to_credentials, emails):
    """Creates a Google Sheet with two tabs for content and structure errors."""
    if content_df.empty and structure_df.empty:
        return "No errors detected. Google Sheet was not created."
    
    # Google Sheets API setup
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(path_to_credentials, scope)
    client = gspread.authorize(creds)
    
    # Create new spreadsheet
    sheet = client.create(sheet_name)
    for email in emails:
        sheet.share(email, perm_type='user', role='writer', notify=False)
    
    # Add error tabs
    if not content_df.empty:
        content_worksheet = sheet.add_worksheet(title="Content Errors", rows="1000", cols="20")
        content_worksheet.update([content_df.columns.values.tolist()] + content_df.values.tolist())
    
    if not structure_df.empty:
        structure_worksheet = sheet.add_worksheet(title="Structure Errors", rows="1000", cols="20")
        structure_worksheet.update([structure_df.columns.values.tolist()] + structure_df.values.tolist())
    
    sheet.del_worksheet(sheet.sheet1)
    return {"sheet_url": sheet.url}

def main_validator(source_folder, emails, destination_folder):
    files = os.listdir(source_folder)
    all_lines = []
    all_metadata = []
    
    for f in files:
        with open(f'{source_folder}/{f}', 'r', encoding='utf-8') as file:
            data = json.load(file)
            all_lines.append(data['data']['messages'])
            all_metadata.append(data['metadata'])



    cnt_turns = Counter()
    for idx, l in enumerate(all_lines):
        ex = []
        buffer = []
        metadata = all_metadata[idx]["data"]["original_uri"]
        for cell_id, element in enumerate(l):
            role = element["role"]
            if role == "User" and buffer:
                ex.append(buffer)
                buffer = []  # every time we see a user, we start a new turn

            content = element["content"]
            if type(content) == dict:
                content = convert_to_text(content)
            elif isinstance(content, str):
                pass
            else:
                raise ValueError(f"Unexpected content type: {type(content)}")

            if role == "User":
                tag = "user_query"
                buffer.append(
                    {"role": "user", "content": content, "tag": tag, "metadata": metadata}
                )
            else:
                try:
                    tag = MAPPING[role]
                    if tag in ["code"]:
                        # process code
                        processed_content = extract_data(content, regex_code, all_metadata[idx]["data"]["original_uri"],cell_id )

                        buffer.append(
                            {
                                "role": "assistant",
                                "content": processed_content,
                                "content_raw": content,
                                "tag": tag,
                                "metadata": metadata,
                            }
                        )
                    elif tag in ["scaffolding_code"]:
                        # process scaffolding code
                        processed_content = extract_data(content, regex_scf, all_metadata[idx]["data"]["original_uri"],cell_id )
                        buffer.append(
                            {
                                "role": "assistant",
                                "content": processed_content,
                                "content_raw": content,
                                "tag": tag,
                                "metadata": metadata,
                            }
                        )
                    else:
                        buffer.append(
                            {
                                "role": "assistant",
                                "content": content,
                                "tag": tag,
                                "metadata": metadata,
                            }
                        )
                except KeyError:
                    errors_task_id.append(all_metadata[idx]['data']['original_uri'])
                    error_cell.append(cell_id)
                    error_msg.append(f"Role/intention error: {role} from {all_metadata[idx]['data']}\t\t{content[:2000]}")

        if buffer:
            ex.append(buffer)
        cnt_turns[len(ex)] += 1

        all_instances.append(ex)

    
    cnt = 0
    total = 0
    good_instances = []
    collab_error_links = []
    error_truns = []
    error_message = []
    for instance in all_instances:
        lengths = [len(x) for x in instance if len(x) != 2 and len(x) != 5]
        # check if the first round is user query
        if instance[0][0]['role'] != 'user':
            #print(f"First round role is not user: {instance[0][0]['role']} {instance[0][0]['metadata']} Content: {instance[0][0]['content'][:20]}")
            err_msg = f"First round role is not user: {instance[0][0]['role']} Content: {instance[0][0]['content'][:20]}"
            turn_count = 1
            collab_error_links.append(instance[0][0]['metadata'])
            error_truns.append(turn_count)
            error_message.append(err_msg) 
            continue
        if not lengths:
            good_instances.append(instance)
            continue
        for idx, turns in enumerate(instance):
            total += 1
            if len(turns) == 2 or len(turns) == 5:
                continue
            cnt += 1
            #print(f"Number of turns error. Expected 2 or 5, got {len(turns)} in turn {idx}. { {instance[0][0]['metadata']} }")
            turn_count=idx
            err_msg=f"Number of turns error. Expected 2 or 5, got {len(turns)} in turn {idx}."
        collab_error_links.append(instance[0][0]['metadata'])
        error_truns.append(turn_count)
        error_message.append(err_msg) 
    
    content_error_df = pd.DataFrame({"id": errors_task_id, "error_cell": error_cell, "error_msg": error_msg})
    structure_error_df = pd.DataFrame({"id": collab_error_links, "error_trun": error_truns, "error_msg": error_message})

    merged_collab_links = errors_task_id + collab_error_links
    collab_links_with_issues_df = pd.DataFrame(merged_collab_links, columns=["collab"])
    colab_column="collab"
    copy_missing_jsons_results = copy_missing_jsons(source_folder, destination_folder, collab_links_with_issues_df, colab_column)

    if content_error_df.empty and structure_error_df.empty:
        return {"status": "success", "destination_folder": destination_folder, "data":copy_missing_jsons_results}
    else:
        results= create_google_sheet("Validation Errors", content_error_df, structure_error_df,GOOGLE_API_CREDENTIALS_PATH, emails)
        return {"status": "failed","sheet_url": results['sheet_url'], "data":copy_missing_jsons_results}
    
    
# path = '/Users/ernestamenyedzi/Documents/Work/Turing/turing-automations/salesforce_v2/delivery_workflow/output/lwc/json_files-10-02-2025'
# clean_path = '/Users/ernestamenyedzi/Documents/Work/Turing/turing-automations/salesforce_v2/delivery_workflow/output/lwc/clean'

# # # path = '/Users/ernestamenyedzi/Documents/Work/Turing/turing-automations/salesforce_v2/delivery_workflow/output/lwc/missing_json_files-05-02-2025'
# emails = ["eamenyedzi@gmail.com", "shaik.m@turing.com", "sheikh.m@turing.com","khaled.d@turing.com", "amod.sardesai@turing.com", "ankit.jasuja@turing.com"]
# t = main_validator(path,emails,clean_path)
# print(t)

