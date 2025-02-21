import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import json

class ApexNotebookExtractor:
    """
    A class to extract various data elements from an Apex Notebook.
    """

    @staticmethod
    def extract_metadata_for_apex(lines):
        """
        Extract metadata from the notebook's first cell, including 'File Name' and 'Number of Issues'.
        """
        if not isinstance(lines, list):
            raise TypeError("Expected a list of lines for metadata extraction.")
        if not lines:
            return {"class_name": "", "number_of_issues": "0"}  # Ensure both fields exist

        metadata = {"class_name": "", "number_of_issues": "0"}  # Initialize

        for line in lines:
            line_cleaned = line.strip()
            if line_cleaned.startswith("**File Name**"):
                metadata["class_name"] = line_cleaned.split("-", 1)[-1].strip()
            elif line_cleaned.startswith("File Name"):
                metadata["class_name"] = line_cleaned.split("-", 1)[-1].strip()
            elif line_cleaned.startswith("**Number of Issues**"):
                metadata["number_of_issues"] = line_cleaned.split("-", 1)[-1].strip()  # Capture issue count
            elif line_cleaned.startswith("Number of Issues"):
                metadata["number_of_issues"] = line_cleaned.split("-", 1)[-1].strip()  # Capture issue count

        return metadata

    @staticmethod
    def extract_data_in_second_or_third_cell(lines, first_block, second_block,first_block_name, second_block_name):
        """
        Extract data between specified blocks in the given lines.
        """
        if not isinstance(lines, list):
            raise TypeError("Expected a list of lines for extraction.")
        if not first_block or not second_block:
            raise ValueError("Both first_block and second_block must be provided.")

        first_block_lines = []
        second_block_lines = []
        capture_first_block = False
        capture_second_block = False

        for line in lines:
            line_cleaned = line.strip()
            # Start capturing the first block
            if line_cleaned.startswith(f"**{first_block}**"):
                capture_first_block = True
                capture_second_block = False
                continue
            # Start capturing the second block
            if line_cleaned.startswith(f"**{second_block}**"):
                capture_first_block = False
                capture_second_block = True
                continue
            # Append lines to the respective block
            if capture_first_block:
                first_block_lines.append(line)
            elif capture_second_block:
                second_block_lines.append(line)

        # Process captured lines and strip unnecessary content
        return {
            first_block_name: '\n'.join(first_block_lines).strip(),
            second_block_name: '\n'.join(second_block_lines).strip()
        }


    @staticmethod
    def extract_method_updates(lines, method_updates):
        """
        Extracts method updates, including method names, issues fixed, and updated code, from the provided lines of notebook content.

        This function processes a list of strings representing notebook content, identifying method-specific details 
        (e.g., method names, issues fixed, and updated code). It appends the extracted details to the provided `method_updates` list.

        Args:
            lines (list): A list of strings where each string represents a line from the notebook content.
            method_updates (list): A list to store the extracted method updates. Each method's details are appended as a dictionary.

        Returns:
            list: The updated `method_updates` list containing dictionaries for each method. 
                  Each dictionary has the following structure:
                  {
                      "Method Name": <method name>,
                      "Issues Fixed In this method": <issues fixed as a string>,
                      "Updated Code": <updated code as a string>
                  }
        """
        if not isinstance(lines, list):
            raise TypeError("Expected a list of lines for method updates extraction.")
        if not isinstance(method_updates, list):
            raise TypeError("Expected method_updates to be a list.")

        current_method = None
        issues_fixed = []
        updated_code_lines = []
        capture_issues = False
        capture_code = False

        for line in lines:
            line_cleaned = line.strip()
            if line_cleaned.startswith("**Method Name**"):
                if current_method:
                    method_updates.append({
                        "Method Name": current_method,
                        "Issues Fixed In this method": '\n'.join(issues_fixed).strip() if issues_fixed else "",
                        "Updated Code": '\n'.join(updated_code_lines).strip() if updated_code_lines else ""
                    })
                    issues_fixed = []
                    updated_code_lines = []
                current_method = line_cleaned.split("-", 1)[-1].strip()
            elif line_cleaned.startswith("**Issues Fixed In this method**"):
                capture_issues = True
                capture_code = False
            elif line_cleaned.startswith("**Updated Code**"):
                capture_issues = False
                capture_code = True
            elif capture_issues:
                issues_fixed.append(line)
            elif capture_code:
                updated_code_lines.append(line)

        if current_method:
            method_updates.append({
                "Method Name": current_method,
                "Issues Fixed In this method": '\n'.join(issues_fixed).strip() if issues_fixed else "",
                "Updated Code": '\n'.join(updated_code_lines).strip() if updated_code_lines else ""
            })

        return method_updates




    @staticmethod
    def extract_to_issues(lines, issues):
        # Join the cell lines into a single string for easier processing
        cell_content = "".join(lines)

        # Define patterns to extract **Error**, **Code**, and **Assistant** sections
        error_pattern = r"\*\*Error\*\*\n(.*?)\*\*Code\*\*"
        code_pattern = r"\*\*Code\*\*\n(.*?)\*\*Assistant\*\*"
        assistant_pattern = r"\*\*Assistant\*\*\n(.*?)$"

        # Extract matches for each section
        error_matches = re.findall(error_pattern, cell_content, re.DOTALL)
        code_matches = re.findall(code_pattern, cell_content, re.DOTALL)
        assistant_matches = re.findall(assistant_pattern, cell_content, re.DOTALL)



        # Ensure all sections have the same length
        for error, code, assistant in zip(error_matches, code_matches, assistant_matches):
            issues.append({
                "user": {
                    "error": error.strip(),
                    "code": code.strip()
                },
                "assistant": assistant.strip()
            })
        
        return issues


    @staticmethod
    def extract_pmd_data(lines):
        """
        Extracts data related to PMD results and explanations from a list of lines in a notebook cell.

        This function processes a list of strings representing notebook content to extract details about 
        the PMD results, specifically the final PMD run status and explanations of PMD errors.

        Args:
            lines (list): A list of strings where each string represents a line from the notebook content.

        Returns:
            dict: A dictionary containing the following keys:
                - "Final PMD Run" (str): The result of the final PMD run, extracted from the notebook content.
                - "Explanation on PMD Errors" (list): A list of explanations for PMD errors, extracted from the notebook content. 
                  Each explanation is captured as a string without leading special characters (e.g., `-` or `*`).

        Example Output:
            {
                "Final PMD Run": "No issues found",
                "Explanation on PMD Errors": ["Error in line 10", "Another issue found in the method"]
            }
        """
        if not isinstance(lines, list):
            raise TypeError("Expected a list of lines for PMD data extraction.")
        if not lines:
            return result  # Return empty result if input is empty

        result = {
            "Final PMD Run": "",
            "Explanation on PMD Errors": []
        }

        capture_explanation = False
        explanation_lines = []

        for line in lines:
            line_cleaned = line.strip()

            # Extract "Final PMD Run"
            if line_cleaned.startswith("**Final PMD Run**"):
                next_line = lines[lines.index(line) + 1].strip()
                result["Final PMD Run"] = next_line.split("-", 1)[-1].strip()

            # Start capturing "Explanation on PMD Errors"
            if line_cleaned.startswith("**Explanation on PMD Errors**"):
                capture_explanation = True
                continue

            # Append lines while capturing explanations
            if capture_explanation:
                if line_cleaned.startswith("-") or line_cleaned.startswith("*"):
                    explanation_lines.append(line_cleaned[1:].strip())

        # Save explanations
        result["Explanation on PMD Errors"] = explanation_lines

        return result

    @staticmethod
    def extract_agentforce_data(lines):
        """
        Extracts data related to Agentforce updates and issues from a list of lines in a notebook cell.

        This function processes notebook cell content to extract information about code updates made by Agentforce, 
        whether the updates match the expected results, and any issues identified in the Agentforce-generated code.

        Args:
            lines (list): A list of strings where each string represents a line from the notebook content.

        Returns:
            dict: A dictionary containing the following keys:
                - "Code Update By Agentforce" (str): A multi-line string of the code updates provided by Agentforce. 
                  The content is captured between the **Code Update By Agentforce** section and the subsequent section.
                - "Is it same as generated by Agentforce" (str): Indicates whether the code matches the expected results.
                  Extracted from the **Is it same as generated by Agentforce?** section.
                - "Issues in Agentforce code" (list): A list of issues identified in the Agentforce-generated code. 
                  Each issue is captured as a string from the **Issues in Agentforce code** section.

        Example Output:
            {
                "Code Update By Agentforce": "System.debug('Agentforce code update');",
                "Is it same as generated by Agentforce": "NO",
                "Issues in Agentforce code": ["Code was too big for Agentforce to generate the output."]
            }
        """
        if not isinstance(lines, list):
            raise TypeError("Expected a list of lines for PMD data extraction.")
        if not lines:
            return result  # Return empty result if input is empty

        result = {
            "Code Update By Agentforce": "",
            "Is it same as generated by Agentforce": "",
            "Issues in Agentforce code": []
        }

        capture_code_update = False
        capture_issues = False
        code_update_lines = []

        for line in lines:
            line_cleaned = line.strip()
            if line_cleaned.startswith("**Code Update By Agentforce**"):
                capture_code_update = True
                capture_issues = False
                continue
            if line_cleaned.startswith("**Is it same as generated by Agentforce?**"):
                capture_code_update = False
                result["Is it same as generated by Agentforce"] = line_cleaned.split("-", 1)[-1].strip()
                continue
            if line_cleaned.startswith("**Issues in Agentforce code**"):
                capture_issues = True
                continue
            if capture_code_update:
                code_update_lines.append(line)
            if capture_issues:
                if line_cleaned.startswith("-"):
                    result["Issues in Agentforce code"].append(line_cleaned[1:].strip())

        result["Code Update By Agentforce"] = '\n'.join(code_update_lines).strip()

        return result

def process_single_notebook(notebook):
    """
    Process a single notebook to extract key metadata, Apex code analysis, updates, 
    and other relevant information.

    Args:
        notebook (dict): A dictionary representing a single notebook with its content 
                         and metadata.

    Returns:
        dict: A dictionary containing the processed results with metadata and extracted data.

        Example Structure:
        {
            "status": "OK",
            "parsed_data": {
                "metadata": { ... },
                "data": {
                    "content_metadata": { ... },
                    "Class Level Update": { ... },
                    "Method Update": [ ... ],
                    "Final PMD Run": "...",
                    "Explanation on PMD Errors": [ ... ]
                }
            }
        }
    """
    if not isinstance(notebook, dict):
        return {"status": "FAILED", "error_msg": "Notebook should be a dictionary."}
    try:
        content_data = json.loads(notebook.get('content', '{}'))
    except json.JSONDecodeError as e:
        return {"status": "FAILED", "error_msg": f"Invalid JSON in notebook content: {str(e)}"}
    
    issues_list = []
    content_data = None
    metadata = {}
    apex_code_data = {}


    try:
        content_data = json.loads(notebook['content'])
        for key, values in content_data.items():
            if key == 'cells':
                for value in values:
                    if value['cell_type'] == 'markdown':
                        source_lines = value['source']
                        
                        # Extract metadata
                        if '**Apex Code Analysis**'.lower() in source_lines[0].lower():
                            metadata = ApexNotebookExtractor.extract_metadata_for_apex(source_lines)
                        
                        # Extract Apex code and related issues
                        elif '**Apex Code**'.lower() in source_lines[0].lower():
                            apex_code_data = ApexNotebookExtractor.extract_data_in_second_or_third_cell(
                                source_lines, 'Apex Code', 'Issues Raised by PMD Code Analyzer', 'class', 'issues'
                            )

                        # Extract method updates
                        elif '**Issue**'.lower() in source_lines[0].lower():
                            ApexNotebookExtractor.extract_to_issues(source_lines, issues_list)


                # Compile extracted data into a structured format
                extracted_data = {
                    "status": "OK",
                    "content_metadata": metadata,
                    **apex_code_data,
                    "conversations": issues_list
                }
                # Remove a __src_sheet_name from a metadata dictionary if it exists.
                notebook_metadata = notebook.get('metadata', {})
                if "data" in notebook_metadata and "__src_sheet_name" in notebook_metadata["data"]:
                    del notebook_metadata["data"]["__src_sheet_name"]
                # Create the final results with metadata
                results = { 
                    'status': "OK",
                    'parsed_data':{
                    'metadata': notebook_metadata,
                    'data': extracted_data
                }}

                return results

    except Exception as exc:
        print(f"Generated an exception: {exc}")
        return {
            "status": "FAILED",
            "error_msg": str(exc),
            "uri": notebook.get('metadata', {}).get('data', {}).get('original_uri', '')
        }



def process_notebook_batch_concurrently(notebook_batch, max_workers=5):
    """
    Process a batch of notebooks concurrently.

    Args:
        notebook_batch (dict): Dictionary containing a list of notebooks under "items".
        max_workers (int): Maximum number of threads to use.

    Returns:
        list: Results from processing each notebook.
    """
    if not isinstance(notebook_batch, dict):
        raise TypeError("Expected a dictionary containing a list of notebooks under 'items'.")
    if 'items' not in notebook_batch or not isinstance(notebook_batch['items'], list):
        raise ValueError("'items' key must be present in notebook_batch and should be a list.")
    if not isinstance(max_workers, int) or max_workers <= 0:
        raise ValueError("max_workers should be a positive integer.")

    notebooks = notebook_batch.get('items', [])
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_notebook = {executor.submit(process_single_notebook, notebook): notebook for notebook in notebooks}

        for future in as_completed(future_to_notebook):
            notebook = future_to_notebook[future]
            try:
                results.append(future.result())
            except Exception as exc:
                print(f"Error processing notebook {notebook.get('metadata', {}).get('id', 'unknown')}: {exc}")
                results.append({"status": "FAILED", "error_msg": str(exc)})

    return results