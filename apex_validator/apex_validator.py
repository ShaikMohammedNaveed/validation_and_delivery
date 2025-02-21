import json
import os
import re
import sys

def load_notebook(file_path):
    """
    Loads and parses a Jupyter/Colab notebook file (.ipynb).
    :param file_path: Path to the notebook file.
    :return: Dictionary containing notebook data (metadata and cells).
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            notebook_data = json.load(file)
        metadata = notebook_data.get("metadata", {})
        cells = notebook_data.get("cells", [])

        return notebook_data, metadata, cells

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None, None, None
    except json.JSONDecodeError:
        print(f"Error: Failed to parse JSON from '{file_path}'. Ensure it's a valid notebook file.")
        return None, None, None

def detect_notebook_type(cells):
    """

    :param cells: List of notebook cells.
    :return: "apex", or None if detection fails.
    """
    for cell in cells[:1]:
        if cell.get("cell_type") == "markdown":
            cell_text = "".join(cell.get("source", [])).lower()

            # Apex Notebook Indicators
            if "apex code analysis" in cell_text:
                return "apex"
            elif "number of issues" in cell_text:
                return "apex"
            elif "file name" in cell_text:
                return "apex"

    return None  # If no clear match is found

def validate_apex_code_block(cells):
    """
    Validates the structure and formatting of the Apex Code section.
    Checks:
    1. Presence and bold formatting of '**Apex Code**'.
    2. Presence of the file name (enclosed in backticks).
    3. Presence of Apex code block (```apex) and ensures no JSON block exists here.
    4. '**Issues Raised by PMD Code Analyzer**' is present and properly formatted.
    5. JSON code block is present only under PMD section.

    :param cells: List of notebook cells.
    :return: List of validation errors.
    """
    validation_errors = []

    # Check only the second cell (index 1)
    if len(cells) < 2 or cells[1].get("cell_type") != "markdown":
        validation_errors.append("❌ Second cell is missing or not a markdown cell.")
        return validation_errors

    cell_text = "".join(cells[1].get("source", [])).strip()

    # 1️⃣ Check for **Apex Code** presence and bold formatting
    if "Apex Code" in cell_text:
        if "**Apex Code**" not in cell_text:
            validation_errors.append(f"❌ 'Apex Code' in Cell 2 is not properly bolded.")
    else:
        validation_errors.append("❌ 'Apex Code' section is missing in Cell 2.")

    # 2️⃣ Check for the presence of the file name (enclosed in backticks)
    file_name_match = re.search(r"`([\w\s^\n]+)`", cell_text)
    file_name_match = re.search(r"`([^\n`]+)`", cell_text)
    # validation_errors.append(file_name_match)
    if not file_name_match:
        validation_errors.append("❌ File name is missing or not enclosed in backticks (`) in Cell 2.")

    # 3️⃣ Check for Apex code block (```apex) and ensure no JSON block exists here
    apex_code_match = re.search(r"```apex[\s\S]*?```", cell_text, re.IGNORECASE)
    json_code_match = re.search(r"```json[\s\S]*?```", cell_text, re.IGNORECASE)

    if apex_code_match:
        if json_code_match and json_code_match.start() < apex_code_match.end():
            validation_errors.append("❌ JSON code block found within Apex code section in Cell 2.")
    else:
        if "```json```":
            validation_errors.append("❌ JSON code block found before Apex code block in Cell 2.")
        validation_errors.append("❌ Apex code block (```apex) is missing in Cell 2.")

    # 4️⃣ Check for '**Issues Raised by PMD Code Analyzer**' (already partially handled)
    if "Issues Raised by PMD Code Analyzer" in cell_text:
        if "**Issues Raised by PMD Code Analyzer**" not in cell_text:
            validation_errors.append("❌ 'Issues Raised by PMD Code Analyzer' in Cell 2 is not properly bolded.")
    else:
        validation_errors.append("❌ 'Issues Raised by PMD Code Analyzer' section is missing in Cell 2.")

    # 5️⃣ Ensure JSON code block is present under the PMD section
    pmd_section_match = re.search(r"\*\*Issues Raised by PMD Code Analyzer\*\*([\s\S]*)", cell_text)
    if pmd_section_match:
        pmd_content = pmd_section_match.group(1)
        if "```json" not in pmd_content:
            validation_errors.append("❌ JSON code block missing under 'Issues Raised by PMD Code Analyzer' in Cell 2.")
    else:
        validation_errors.append("❌ PMD section not properly formatted in Cell 2.")

    return validation_errors

def validate_dynamic_issues(cells):

    """
    Validates dynamic Issue blocks after the Apex code section.
    Enforces the presence, order, and formatting of:
    1. '**Issue** - X'
    2. '**User**'
    3. '**Error**' (with JSON code block)
    4. '**Code**' (with Apex code block)
    5. '**Assistant**' (with Apex code block)

    :param cells: List of notebook cells.
    :return: List of validation errors.
    """

    validation_errors = []
    issue_number = 0

    for index, cell in enumerate(cells):
        if cell.get("cell_type") != "markdown":
            continue

        cell_text = "".join(cell.get("source", [])).strip()

        # Split issues if multiple are in the same cell
        issue_blocks = re.split(r"(\*\*Issue\*\*\s*[-:]?\s*\d+)", cell_text)

        # Process each Issue block
        for i in range(1, len(issue_blocks), 2):
            issue_header = issue_blocks[i]
            issue_content = issue_blocks[i + 1] if i + 1 < len(issue_blocks) else ""

            # Validate Issue Header
            issue_match = re.search(r"\*\*Issue\*\*\s*[-:]?\s*(\d+)", issue_header)
            if issue_match:
                issue_number = int(issue_match.group(1))

                # 1️⃣ Validate **Issue Header**
                if "**Issue**" not in issue_header:
                    validation_errors.append(f"❌ 'Issue' is not properly bolded in Issue {issue_number} (Cell {index + 1}).")

                # 2️⃣ Validate **User Section**
                if "**User**" not in issue_content:
                    validation_errors.append(f"❌ 'User' section missing or not bolded in Issue {issue_number} (Cell {index + 1}).")

                # 3️⃣ Validate **Error Section** with JSON Block
                error_section = re.search(r"\*\*Error\*\*([\s\S]*?)(\*\*Code\*\*|\*\*Assistant\*\*|$)", issue_content)
                if error_section:
                    if "**Error**" not in error_section.group(0):
                        validation_errors.append(f"❌ 'Error' is not properly bolded in Issue {issue_number} (Cell {index + 1}).")

                    json_match = re.search(r"```json[\s\S]*?```", error_section.group(1))
                    if not json_match:
                        validation_errors.append(f"❌ Missing JSON code block under 'Error' in Issue {issue_number} (Cell {index + 1}).")

                    apex_match = re.search(r"```apex[\s\S]*?```", error_section.group(1))
                    if apex_match:
                        validation_errors.append(f"❌ Apex code block found under 'Error' in Issue {issue_number} (Cell {index + 1}).")
                else:
                    validation_errors.append(f"❌ 'Error' section missing in Issue {issue_number} (Cell {index + 1}).")

                # 4️⃣ Validate **Code Section** with Apex Block
                code_section = re.search(r"\*\*Code\*\*([\s\S]*?)(\*\*Assistant\*\*|$)", issue_content)
                if code_section:
                    if "**Code**" not in code_section.group(0):
                        validation_errors.append(f"❌ 'Code' is not properly bolded in Issue {issue_number} (Cell {index + 1}).")

                    apex_match = re.search(r"```apex[\s\S]*?```", code_section.group(1))
                    if not apex_match:
                        validation_errors.append(f"❌ Missing Apex code block under 'Code' in Issue {issue_number} (Cell {index + 1}).")

                    json_match = re.search(r"```json[\s\S]*?```", code_section.group(1))
                    if json_match:
                        validation_errors.append(f"❌ JSON code block found under 'Code' in Issue {issue_number} (Cell {index + 1}).")
                else:
                    validation_errors.append(f"❌ 'Code' section missing in Issue {issue_number} (Cell {index + 1}).")

                # 5️⃣ Validate **Assistant Section** with Apex Block
                assistant_section = re.search(r"\*?\*?Assistant\*?\*?([\s\S]*?)($|\*?\*?Issue\*?\*?)", issue_content)
                if assistant_section:
                    if "**Assistant**" not in assistant_section.group(0):
                        validation_errors.append(f"❌ 'Assistant' is not properly bolded in Issue {issue_number} (Cell {index + 1}).")

                    apex_match = re.search(r"```apex[\s\S]*?```", assistant_section.group(1))
                    if not apex_match:
                        validation_errors.append(f"❌ Missing Apex code block under 'Assistant' in Issue {issue_number} (Cell {index + 1}).")

                    json_match = re.search(r"```json[\s\S]*?```", assistant_section.group(1))
                    if json_match:
                        validation_errors.append(f"❌ JSON code block found under 'Assistant' in Issue {issue_number} (Cell {index + 1}).")
                else:
                    validation_errors.append(f"❌ 'Assistant' section missing in Issue {issue_number} (Cell {index + 1}).")

            else:
                validation_errors.append(f"❌ Issue header missing or incorrectly formatted in Cell {index + 1}.")

    return validation_errors

def extract_issue_count(text):
    """
    Extracts the 'Number of Issues' value from a markdown cell, handling extra spaces and variations.
    
    :param text: Markdown content.
    :return: Extracted issue count (int) or None if not found.
    """
    # Normalize text by removing excessive spaces
    normalized_text = " ".join(text.split()).strip().lower()

    # Updated regex to handle different variations
    match = re.search(r"\*\*number of issues\*\*\s*[-:]?\s*(\d+)", normalized_text, re.IGNORECASE)
    
    return int(match.group(1)) if match else None

def validate_issue_count(cells, notebook_type):
    """
    Validates that the declared issue count matches the number of user-assistant response pairs 
    and checks for properly labeled and present code/error blocks.

    :param cells: List of notebook cells.
    :param notebook_type: "apex".
    :return: List of validation errors.
    """
    validation_errors = []

    declared_issue_count = None
    issue_count_cell = None
    issues = {}  # Tracks each issue

    issue_number = None
    error_expected = False
    code_expected = False

    for index, cell in enumerate(cells):
        cell_text = "".join(cell.get("source", [])).strip()

        if cell.get("cell_type") == "markdown":
            # Extract "Number of Issues"
            match = re.search(r"\*\*number of issues\*\*\s*[-:]?\s*(\d+)", cell_text, re.IGNORECASE)
            if match:
                declared_issue_count = int(match.group(1))
                issue_count_cell = index + 1

            # Detect Issue Headers
            issue_match = re.search(r"\*\*Issue\*\*\s*[-:]?\s*(\d+)", cell_text, re.IGNORECASE)
            if issue_match:
                issue_number = int(issue_match.group(1))
                issues[issue_number] = {
                    "user": False, "assistant": False,
                    "error_label": False, "error_block": False,
                    "code_label": False, "code_block": False
                }
                error_expected = False
                code_expected = False

            # Track User and Assistant presence
            if "**User**" in cell_text and issue_number is not None:
                issues[issue_number]["user"] = True
                error_expected = True  # Expect an error block next

            if "**Assistant**" in cell_text and issue_number is not None:
                issues[issue_number]["assistant"] = True

            # Detect Error & Code Labels
            if "**Error**" in cell_text and issue_number is not None:
                issues[issue_number]["error_label"] = True
                error_expected = True

            if "**Code**" in cell_text and issue_number is not None:
                issues[issue_number]["code_label"] = True
                code_expected = True

        elif cell.get("cell_type") == "code" and issue_number is not None:
            # Check the actual content of the code cell
            code_content = cell_text.strip().lower()

            # Detect JSON Error block
            if error_expected and code_content.startswith("[") and code_content.endswith("]"):
                issues[issue_number]["error_block"] = True
                error_expected = False  # Reset expectation

            # Detect Apex Code block
            if code_expected and (
                "public class" in code_content or "trigger " in code_content or "system.debug" in code_content
            ):
                issues[issue_number]["code_block"] = True
                code_expected = False  # Reset expectation

    # Find issues missing required components
    missing_users = [issue for issue in issues if not issues[issue]["user"]]
    missing_assistants = [issue for issue in issues if not issues[issue]["assistant"]]
    missing_error_labels = [issue for issue in issues if not issues[issue]["user"] and not issues[issue]["error_label"]]
    missing_error_blocks = [issue for issue in issues if not issues[issue]["error_label"] and not issues[issue]["error_block"]]
    missing_code_labels = [issue for issue in issues if not issues[issue]["user"] and not issues[issue]["code_label"]]
    missing_code_blocks = [issue for issue in issues if not issues[issue]["code_label"] and not issues[issue]["code_block"]]

    # Report issues found
    if missing_users:
        validation_errors.append(f"⚠️ Missing User response(s) in Issue(s): {', '.join(map(str, missing_users))}")
    if missing_assistants:
        validation_errors.append(f"⚠️ Missing Assistant response(s) in Issue(s): {', '.join(map(str, missing_assistants))}")
    if missing_error_labels:
        validation_errors.append(f"⚠️ Missing **Error** label in Issue(s): {', '.join(map(str, missing_error_labels))}")
    if missing_error_blocks:
        validation_errors.append(f"❌ Missing properly formatted Error block(s) in Issue(s): {', '.join(map(str, missing_error_blocks))}")
    if missing_code_labels:
        validation_errors.append(f"⚠️ Missing **Code** label in Issue(s): {', '.join(map(str, missing_code_labels))}")
    if missing_code_blocks:
        validation_errors.append(f"❌ Missing properly formatted Code block(s) in Issue(s): {', '.join(map(str, missing_code_blocks))}")

    # Validate overall issue count
    actual_issue_count = len(issues)
    
    if declared_issue_count is None:
        validation_errors.append("⚠️ Warning: 'Number of Issues' not found or incorrectly formatted.")
    elif declared_issue_count != actual_issue_count:
        validation_errors.append(
            f"❌ Mismatch: Declared issues ({declared_issue_count}) ≠ Actual user-assistant pairs ({actual_issue_count}) Please check cell number 1."
        )
    elif issue_count_cell and issue_count_cell > min(issues.keys()):
        validation_errors.append(
            f"⚠️ Warning: 'Number of Issues' declaration should be placed before user-assistant conversations (Cell {issue_count_cell})."
        )

    return validation_errors

def validate_notebook_structure(cells, notebook_type):
    validation_errors = []

    expected_sections = {
        "apex": ["Apex Code Analysis", "Apex Code", "Issues Raised by PMD Code Analyzer"]
    }

    found_sections = []

    for cell in cells:
        if cell.get("cell_type") == "markdown":
            # Normalize spaces and remove unnecessary Markdown symbols
            cell_text = " ".join("".join(cell.get("source", [])).split()).strip().lower()
            cell_text = cell_text.replace("**", "").replace("#", "")  # Remove bold and header markers

            for section in expected_sections[notebook_type]:
                if section.lower() in cell_text and section not in found_sections:
                    found_sections.append(section)

    # Validate section presence
    missing_sections = [section for section in expected_sections[notebook_type] if section not in found_sections]
    if missing_sections:
        validation_errors.append(f"Missing required sections: {', '.join(missing_sections)}")

    return validation_errors

def validate_apex_metadata_formatting(cells):
    """
    Validates the presence and bold formatting of critical metadata like:
    - **Apex Code Analysis**
    - **File Name** (and checks if an actual file name is provided)

    This validation is restricted to the first cell only.
    
    :param cells: List of notebook cells.
    :return: List of validation errors, if any.
    """
    validation_errors = []

    # Check only the first cell (cell index 0)
    if not cells or cells[0].get("cell_type") != "markdown":
        validation_errors.append("❌ First cell is missing or not a markdown cell.")
        return validation_errors

    cell_text = "".join(cells[0].get("source", [])).strip()

    # Check for **Apex Code Analysis**
    if "Apex Code Analysis" in cell_text:
        if "**Apex Code Analysis**" not in cell_text:
            validation_errors.append(f"❌ 'Apex Code Analysis' in Cell 1 is not properly bolded.")
    else:
        validation_errors.append("❌ 'Apex Code Analysis' section is missing in Cell 1.")

    # Check for **File Name**
    if "File Name" in cell_text:
        if "**File Name**" not in cell_text:
            validation_errors.append(f"❌ 'File Name' in Cell 1 is not properly bolded.")

        # Check if a file name is provided after the dash
        file_name_match = re.search(r"\*\*File Name\*\*\s*[-:]?\s*(\w+)", cell_text)
        if not file_name_match:
            validation_errors.append(f"⚠️ No file name provided after 'File Name' in Cell 1.")
    else:
        validation_errors.append(f"❌ 'File Name' section is missing in Cell 1.")

    return validation_errors


def validate_content_formatting(cells, notebook_type):
    """
    Validates the content formatting inside notebook sections.

    :param cells: List of notebook cells.
    :param notebook_type: String representing the notebook type (e.g., 'apex').
    :return: A list of validation errors (if any), else an empty list.
    """
    validation_errors = []
    
    apex_patterns = {
        "pmd_json_block": re.compile(r"```[jJ][sS][oO][nN]"),  # Valid JSON block
        "code_blocks": re.compile(r"```apex"),                # Apex code block
    }

    for i, cell in enumerate(cells):
        if cell.get("cell_type") == "markdown":
            cell_text = "".join(cell.get("source", [])).strip()

            #-------------------------------------------
            # 1) Apex- or JSON-specific checks (optional)
            #-------------------------------------------
            if notebook_type == "apex":
                # If a cell has the PMD heading, require a ```json block.
                if "**Issues Raised by PMD Code Analyzer**" in cell_text:
                    if not apex_patterns["pmd_json_block"].search(cell_text):
                        validation_errors.append(
                            f"Invalid JSON formatting declaration (` ```json `) in cell #{i+1}"
                        )

                # If triple backticks appear but no ` ```json ` code block, warn
                elif "```" in cell_text and not apex_patterns["pmd_json_block"].search(cell_text):
                    validation_errors.append(
                        f"Expected ` ```json ` code block in cell #{i+1}"
                    )

                # Check for Apex code blocks
                if "```" in cell_text and not apex_patterns["code_blocks"].search(cell_text):
                    validation_errors.append(
                        f"Apex code block missing correct declaration (` ```apex `) in cell #{i+1}"
                    )

            #-------------------------------------------
            # 2) Generic triple-backtick balance check
            #-------------------------------------------
            tick_count = cell_text.count("```")
            
            # If there are zero triple backticks, just continue
            if tick_count == 0:
                continue
            
            # If the number of triple backticks is odd, it means an unbalanced code fence.
            if tick_count % 2 != 0:
                validation_errors.append(
                    f"Possible missing or unbalanced triple-backtick closure in cell #{i+1}"
                )

    return validation_errors

def validate_static_bold_formatting(cells, notebook_type):
    """
    Validates that specific static and repeating words in the notebook are correctly bolded.

    Expected Bold Words:
    - **Apex Code Analysis**
    - **File Name**
    - **Number of Issues**
    - **Apex Code**
    - **Issues Raised by PMD Code Analyzer**
    - **Issue** - X (Dynamic but required)
    - **User**
    - **Error**
    - **Code**
    - **Assistant**

    :param cells: List of notebook cells.
    :return: A list of validation errors.
    """
    validation_errors = []

    # Define static headers that must always be bold (multi-word phrases).
    required_bold_headers = {
        "Apex Code Analysis",
        "File Name",
        "Number of Issues",
        "Issues Raised by PMD Code Analyzer",
    }

    # Define issue-related headers that should always be bold (single words).
    issue_related_headers = {"Issue", "User", "Error", "Code", "Assistant", "Apex Code",}

    for index, cell in enumerate(cells):
        if cell.get("cell_type") == "markdown":
            cell_text = "".join(cell.get("source", [])).strip()

            missing_headers = []

            # 1) Check required static headers are properly bolded
            for header in required_bold_headers:
                if header in cell_text:
                    bolded_header = f"**{header}**"
                    if bolded_header not in cell_text:
                        missing_headers.append(header)

            # 2) Extract all bolded text segments
            bolded_segments = re.findall(r'\*\*(.*?)\*\*', cell_text)

            # 3) Check issue-related headers
            for issue_header in issue_related_headers:
                # Find all occurrences of the issue header as whole words
                matches = re.finditer(rf"\b{re.escape(issue_header)}\b", cell_text)
                for match in matches:
                    # Check if this occurrence is within any bolded segment
                    is_bolded = False
                    for segment in bolded_segments:
                        # Use word boundaries to ensure exact match within the segment
                        if re.search(rf"\b{re.escape(issue_header)}\b", segment):
                            is_bolded = True
                            break
                    if not is_bolded:
                        missing_headers.append(issue_header)
                        break  # No need to check further if one instance is unbolded

            if missing_headers:
                headers_str = ", ".join(missing_headers)
                validation_errors.append(
                    f"❌ Incorrect bold formatting in Cell {index + 1}: {headers_str}"
                )

    return validation_errors

def validate_issue_block_headers(cells):
    """
    Only validates that certain words ('Issue', 'User', 'Error', 'Code', 'Assistant')
    are correctly bolded *inside Issue block cells*.

    A cell is considered an Issue block cell if it contains the text '**Issue** - '.
    For each such cell:
      - If 'Issue', 'User', 'Error', 'Code', or 'Assistant' appear (whole word),
        check that they appear inside double-asterisk bold markers.
      - If they are not bolded, report that header as missing or unbolded for that cell.

    Returns: a list of descriptive error strings.
    """
    validation_errors = []

    # Words that must be bolded inside any "Issue block" cell
    issue_block_headers = {"Issue", "User", "Error", "Code", "Assistant"}

    for index, cell in enumerate(cells):
        if cell.get("cell_type") != "markdown":
            continue  # Only check markdown cells

        cell_text = "".join(cell.get("source", [])).strip()

        # Detect if this cell is an "Issue block" cell
        # We'll define it simply as containing "**Issue** - "
        if "**Issue** - " not in cell_text:
            continue  # Skip checking single-word boldness for non-issue-block cells

        # For each of the relevant words, if it appears as a whole word, ensure it's bolded
        # Keep track of which headers are missing bold
        missing_bold_headers = []

        for header in issue_block_headers:
            # We'll look for any occurrence of the word (with boundaries),
            # then verify if we have '**header**' for that word in the cell
            pattern_word = rf"\b{re.escape(header)}\b"
            if re.search(pattern_word, cell_text, flags=re.IGNORECASE):
                # The word appears in some form. Now check if it's properly bolded.
                # Note: We do a case-insensitive word check, but require EXACT asterisks "**Header**" in the text
                bolded_form = f"**{header}**"
                # To handle case sensitivity of the actual text, you might want to unify them,
                # but often you want the exact case to match. We'll assume exact case for the final check.
                if bolded_form not in cell_text:
                    missing_bold_headers.append(header)

        if missing_bold_headers:
            # Summarize them in one line
            headers_str = ", ".join(missing_bold_headers)
            validation_errors.append(
                f"❌ In cell {index+1}, the following Issue-block headers are not bolded correctly: {headers_str}"
            )

    return validation_errors

def validate_notebooks_in_folder(folder_path):
    """
    Validates all Jupyter/Colab notebooks in a folder, prints a summary of how many
    passed vs. failed, lists the file names of failing notebooks, and writes a
    summary file containing all issues from all notebooks.
    
    :param folder_path: Path to the directory containing .ipynb notebook files.
    """
    if not os.path.isdir(folder_path):
        print("Error: Invalid folder path.")
        return

    # Get all .ipynb files in the folder
    notebook_files = [f for f in os.listdir(folder_path) if f.endswith(".ipynb")]

    if not notebook_files:
        print("No Jupyter notebooks found in the folder.")
        return

    print(f"\n📂 Found {len(notebook_files)} notebooks in '{folder_path}'. Processing...\n")

    # Pass/fail counters
    colab_pass_count = 0
    colab_fail_count = 0
    other_pass_count = 0
    other_fail_count = 0

    # This will store all issues in the form: [(filename, [list_of_errors]), ...]
    all_issues = []

    for file in notebook_files:
        file_path = os.path.join(folder_path, file)
        
        
        # Run the notebook validation
        notebook_type, errors = generate_validation_report(file_path)
        print("\n" + "-" * 50)

        # If generate_validation_report returns no type, skip counting
        if not notebook_type:
            continue

        # Decide if it's a 'colab' or 'other' type
        # Adjust logic here if detect_notebook_type gives different strings
        if notebook_type.lower() == "apex":
            if errors:
                colab_fail_count += 1
            else:
                colab_pass_count += 1
        else:
            if errors:
                other_fail_count += 1
            else:
                other_pass_count += 1

        # If there are errors, record them for the summary
        if errors:
            all_issues.append((file, errors))

    # Print a final, high-level summary
    total_notebooks = len(notebook_files)
    total_colab = colab_pass_count + colab_fail_count

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY\n")
    print(f"Total files in folder: {total_notebooks}")
    print(f"  - Number of Apex notebooks: {total_colab} (Passed: {colab_pass_count}, Failed: {colab_fail_count})")

    # Print out any failing notebooks
    if all_issues:
        print("\nThe following notebooks had issues:\n")
        for (filename, errs) in all_issues:
            print(f"- {filename} => {len(errs)} error(s)")
    else:
        print("\nNo notebooks had issues.\n")
    print("=" * 60)

    # Generate a single summary file for all issues
    summary_filename = os.path.join(folder_path, "validation_summary.txt")
    with open(summary_filename, "w", encoding="utf-8") as sum_file:
        sum_file.write("VALIDATION SUMMARY\n\n")
        sum_file.write(f"Total notebooks processed: {total_notebooks}\n")
        sum_file.write(f"  - Colab notebooks: {total_colab} (Passed: {colab_pass_count}, Failed: {colab_fail_count})\n")

        if all_issues:
            sum_file.write("The following notebooks had issues:\n\n")
            for (filename, errs) in all_issues:
                sum_file.write(f"{filename}:\n")
                for e in errs:
                    sum_file.write(f"   - {e}\n")
                sum_file.write("\n")
        else:
            sum_file.write("No notebooks had issues.\n")

    print(f"\nA summary of all issues has been saved to '{summary_filename}'\n")

def generate_validation_report(file_path):
    """
    Runs all validation checks and generates a structured validation report.
    Returns (notebook_type, errors).
    
    :param file_path: Path to the notebook file.
    :return: (str notebook_type, list validation_errors)
    """
    notebook_data, metadata, cells = load_notebook(file_path)

    if not notebook_data:
        print("Error: Failed to load the notebook.")
        return (None, [])

    notebook_type = detect_notebook_type(cells)

    if not notebook_type:
        # print("Error: Could not determine notebook type (Expecting APEX notebook).")
        return (None, [])
    print(f"\n📄 Validating: {file_path}")
    print(f"\n📘 Detected Notebook Type: {notebook_type.upper()}")

    validation_errors = []
    metadata_errors = validate_apex_metadata_formatting(cells)
    apex_code_errors = validate_apex_code_block(cells)
    issue_errors = validate_issue_count(cells, notebook_type)
    structure_errors = validate_notebook_structure(cells, notebook_type)
    content_errors = validate_content_formatting(cells, notebook_type)
    issue_block_errors = validate_issue_block_headers(cells)
    dynamic_issue_errors = validate_dynamic_issues(cells)

    # Collect all errors
    validation_errors.extend(metadata_errors)
    validation_errors.extend(apex_code_errors)
    validation_errors.extend(issue_errors)
    validation_errors.extend(structure_errors)
    validation_errors.extend(content_errors)
    validation_errors.extend(issue_block_errors)
    validation_errors.extend(dynamic_issue_errors)
    error_filename = os.path.splitext(file_path)[0] + "_errors.txt"
    if validation_errors:
        print("\n❌ Validation Errors Found:")
        for error in validation_errors:
            print(f"- {error}")
        with open(error_filename, "w", encoding="utf-8") as f:
            f.write(f"Validation Report for {os.path.basename(file_path)}\n")
            f.write(f"Detected Notebook Type: {notebook_type.upper()}\n\n")
            f.write("❌ Validation Errors Found:\n")
            for error in validation_errors:
                f.write(f"- {error}\n")
        print(f"📝 Errors saved to: {error_filename}")
    else:
        if os.path.exists(error_filename):
            os.remove(error_filename)
        print("\n✅ No issues, all good!")

    return (notebook_type, validation_errors)

def handle_input():
    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_file_or_folder>")
        print("Provide a valid file (.ipynb) or folder path containing notebooks.")
        sys.exit(1)

    path = sys.argv[1]

    if os.path.isfile(path):
        generate_validation_report(path)
    elif os.path.isdir(path):
        validate_notebooks_in_folder(path)
    else:
        print("Invalid path provided.\n")
        print("Usage: python script.py <path_to_file_or_folder>")
        print("Ensure the path is correct and try again.")
        sys.exit(1)
        
if __name__ == "__main__":
    handle_input()