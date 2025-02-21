import copy
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import nbformat
from fuzzywuzzy import fuzz
from tqdm.auto import tqdm
import re

class Parser:
    def __init__(self, user="User", assistant="Assistant", max_workers=10):
        self.max_workers = max_workers
        self.user = user
        self.assistant = assistant
        self.sub_roles = ["Clarification Question", "Blueprint", "Implementation plan", "Scaffolding code", "Code"]

    def get_closest_match(self, query, choices):
        """
        Get the closest match(es) to a query string from a list of choices.

        :param query: The query string.
        :param choices: A list of strings to match against.
        """
        best_role = None
        best_score = 0
        for choice in choices:
            score = fuzz.ratio(query, choice)
            if score > best_score and score > 25:
                best_score = score
                best_role = choice

        return best_role, best_score

    def count_empty_from_end(self, cells):
        count = 0
        for message in reversed(cells):
            if message["source"].strip() == "":
                count += 1
            else:
                break
        return count

    def extract_messages(self, notebook):
        """
        Parse a notebook and extract the message objects.

        :param notebook: The notebook object.
        """
        messages = []
        cut_tail = self.count_empty_from_end(notebook.cells)
        cells = notebook.cells[2:]
        if cut_tail:
            cells = cells[:-cut_tail]

        current_role = None
        user_role_count = 0  # Counter for the user role

        for cell in cells:
            if cell["cell_type"] == "markdown":
                headers = [f"**{self.user}**", f"**{self.assistant}**"] + [
                    f"**{sub}**" for sub in self.sub_roles
                ]
            else:
                raise Exception(f'Unknown cell typ  e {cell["cell_type"]}')
            lines = cell["source"].strip().split("\n")
            first_line = lines[0]

            if lines[0].strip() == "**Assistant**":
                first_line = lines[2]
            role, score = self.get_closest_match(first_line, headers)
            if score > 80:
                valid_role = role.replace("*", "").replace("#", "").strip()
                content = "\n".join(lines[1:]).strip("\n")
            else:
                valid_role = current_role
                content = cell["source"]

            # Check if valid_role matches sub-roles, assign `Assistant - SubRole`
            if valid_role in self.sub_roles:
                valid_role = f"{self.assistant} - {valid_role}"

            # Increment user role counter
            if valid_role == self.user:
                user_role_count += 1

            if valid_role == 'Assistant - Blueprint' or valid_role == 'Assistant - Implementation plan':
                if lines[0].strip() == "**Assistant**":
                    content = "\n".join(lines[3:]).strip("\n")

                categorized = self.create_main_heading_json(content)
                cleaned_content = self.clean_content(content)
                parsed_content = self.populate_final_json_from_cleaned_content(cleaned_content, categorized)
                parsed_content = self.remove_numerical_values(parsed_content)
                messages.append(
                    {
                        "role": valid_role,
                        "content": parsed_content,
                        "type": cell["cell_type"],
                    }
                )
            else:
                messages.append(
                    {"role": valid_role, "content": content, "type": cell["cell_type"]}
                )

        current_role = valid_role

        return {
            "messages": messages,
            "number_of_turns": user_role_count,
        }
    
    def remove_numerical_values(self, data):

        def process_text(text):
            # Remove patterns like "1.", "2.", "1 2 3" or standalone numbers, and extra spaces after periods
            text = re.sub(r'(\b\d+\.\s?|\b\d+\b)', '', text).strip()
            return re.sub(r'\s+\.', '.', text)  # Remove extra spaces before periods

        def process_item(item):
            for key, value in item.items():
                if isinstance(value, str):
                    item[key] = process_text(value)
            return item

        for key, value in data.items():
            if isinstance(value, list):
                data[key] = [process_item(item) for item in value]

        return data
    
    def extract_main_headings(self, content):
        """
        Extracts all headings and subheadings from the content marked with '**' at both the start and end.
        """
        return re.findall(r'\*\*(.*?)\*\*', content)

    def create_main_heading_json(self, content):
        """
        Extract main headings and subheadings, categorizing them into respective lists.
        """
        main_headings = self.extract_main_headings(content)
        overview = []  # List to store Overview-related keys
        content_requirements = []  # List for Content Requirements-related keys
        component_communication = []  # List for Component Communication-related keys

        current_main_heading = None

        for heading in main_headings:
            key = heading.lower().replace(" ", "")

            if key == "overview":
                # Switch to Overview
                current_main_heading = "overview"
            elif key == "contentrequirements":
                # Switch to Content Requirements
                current_main_heading = "contentrequirements"
            elif key == "componentcommunication":
                # Switch to Component Communication
                current_main_heading = "componentcommunication"
            else:
                # Add subheading to the current main heading
                if current_main_heading == "overview":
                    overview.append(heading)
                elif current_main_heading == "contentrequirements":
                    content_requirements.append(heading)
                elif current_main_heading == "componentcommunication":
                    component_communication.append(heading)

        return {
            "overview": overview,
            "content_requirements": content_requirements,
            "component_communication": component_communication
        }

    def clean_content(self, content):
        """
        Clean the content by removing unwanted characters like \n and :
        """
        return re.sub(r"[:\n]+", " ", content).strip()


    def clean_sections(self,sections):
        """
        Clean sections by removing " - " and stripping unnecessary whitespace.
        """
        cleaned_sections = []
        for section in sections:
            # Remove " - " and strip leading/trailing whitespace
            cleaned_section = section.replace(" - ", "").strip()
            if cleaned_section:  # Only include non-empty sections
                cleaned_sections.append(cleaned_section)
        return cleaned_sections


    def populate_final_json_from_cleaned_content(self, cleaned_content, categorized):
        """
        Populate the final JSON structure from cleaned content.
        """
        
        sections = re.split(r"\*\*(.*?)\*\*", cleaned_content)
        sections = self.clean_sections(sections)
        sections = [section.strip() for section in sections if section.strip()]
        result = self.parse_mapping(sections, categorized)
        return result

    # Function to process a section
    def parse_section(self, key, fields, data, start_index, section_keys):
        """
        Parse a section of data according to the fields in the mapping.
        Detect transitions between sections to avoid extraneous content in fields.
        """
        result = []
        current_dict = {}

        for field in fields:
            # Find the field in the data starting from the current start_index
            if field in data[start_index:]:
                current_index = data[start_index:].index(field) + start_index

                # If 'Name' is encountered, start a new dictionary
                if field == "Name" and current_dict:
                    result.append(current_dict)
                    current_dict = {}

                # Capture the value for the field
                if current_index + 1 < len(data):
                    next_item = data[current_index + 1]
                    combined_value = next_item
                    next_index = current_index + 2

                    # Combine values until encountering a section key or end of data
                    while (
                        next_index < len(data)
                        and data[next_index] not in section_keys
                        and data[next_index] not in fields
                    ):
                        combined_value += " " + data[next_index]
                        next_index += 1

                    current_dict[field] = combined_value.strip()

                # Update the start index
                start_index = current_index + 1

        # Add the last dictionary if it has content
        if current_dict:
            result.append(current_dict)

        return result, start_index


    def parse_mapping(self, data, mapping):
        """
        Parse the entire data according to the mapping structure,
        ensuring clean transitions between sections.
        """
        result = {}
        start_index = 0
        section_keys = {key.replace("_", " ").title() for key in mapping.keys()}

        for key, fields in mapping.items():
            parsed_section, start_index = self.parse_section(
                key, fields, data, start_index, section_keys
            )
            result[key] = parsed_section

        return result



    def extract_metadata(self, notebook):
        """
        Extract metadata from the notebook's first cell, handling all possible heading formats including #, ##, ###, ####, #####, and their combinations with **.
        """
        notebook_cell = notebook.cells[0]
        lines = notebook_cell["source"].split("\n")
        metadata = {
            "topic": "",
            "message": "",
            "problem_statement": "",
            "required_metadata": [],
            "expected_outcomes": [],
            "manualSetupRequired": [],
            "screenshot": "",
            "tags": []
        }

        current_section = None  # Tracks the active section being parsed

        for line in lines:
            line_cleaned = line.strip()

            # Check for different heading possibilities and assign current_section
            if (
                line_cleaned.startswith("**Topic**")
                or line_cleaned.startswith("# Topic")
                or line_cleaned.startswith("## Topic")
                or line_cleaned.startswith("### Topic")
                or line_cleaned.startswith("#### Topic")
                or line_cleaned.startswith("##### Topic")
                or line_cleaned.startswith("# **Topic**")
                or line_cleaned.startswith("## **Topic**")
                or line_cleaned.startswith("### **Topic**")
                or line_cleaned.startswith("#### **Topic**")
                or line_cleaned.startswith("##### **Topic**")
            ):
                current_section = "topic"
                metadata["topic"] = line_cleaned.split(" - ", 1)[1].strip() if " - " in line_cleaned else ""
            elif (
                line_cleaned.startswith("**Message**")
                or line_cleaned.startswith("# Message")
                or line_cleaned.startswith("## Message")
                or line_cleaned.startswith("### Message")
                or line_cleaned.startswith("#### Message")
                or line_cleaned.startswith("##### Message")
                or line_cleaned.startswith("# **Message**")
                or line_cleaned.startswith("## **Message**")
                or line_cleaned.startswith("### **Message**")
                or line_cleaned.startswith("#### **Message**")
                or line_cleaned.startswith("##### **Message**")
            ):
                current_section = "message"
                metadata["message"] = line_cleaned.split(" - ", 1)[1].strip() if " - " in line_cleaned else ""
            elif (
                line_cleaned.startswith("**Problem Statement**")
                or line_cleaned.startswith("# Problem Statement")
                or line_cleaned.startswith("## Problem Statement")
                or line_cleaned.startswith("### Problem Statement")
                or line_cleaned.startswith("#### Problem Statement")
                or line_cleaned.startswith("##### Problem Statement")
                or line_cleaned.startswith("# **Problem Statement**")
                or line_cleaned.startswith("## **Problem Statement**")
                or line_cleaned.startswith("### **Problem Statement**")
                or line_cleaned.startswith("#### **Problem Statement**")
                or line_cleaned.startswith("##### **Problem Statement**")
            ):
                current_section = "problem_statement"
            elif (
                line_cleaned.startswith("**Required Metadata Before Executing the Code**")
                or line_cleaned.startswith("# Required Metadata Before Executing the Code")
                or line_cleaned.startswith("## Required Metadata Before Executing the Code")
                or line_cleaned.startswith("### Required Metadata Before Executing the Code")
                or line_cleaned.startswith("#### Required Metadata Before Executing the Code")
                or line_cleaned.startswith("##### Required Metadata Before Executing the Code")
                or line_cleaned.startswith("# **Required Metadata Before Executing the Code**")
                or line_cleaned.startswith("## **Required Metadata Before Executing the Code**")
                or line_cleaned.startswith("### **Required Metadata Before Executing the Code**")
                or line_cleaned.startswith("#### **Required Metadata Before Executing the Code**")
                or line_cleaned.startswith("##### **Required Metadata Before Executing the Code**")
            ):
                current_section = "required_metadata"
            elif (
                line_cleaned.startswith("**Expected Outcomes**")
                or line_cleaned.startswith("# Expected Outcomes")
                or line_cleaned.startswith("## Expected Outcomes")
                or line_cleaned.startswith("### Expected Outcomes")
                or line_cleaned.startswith("#### Expected Outcomes")
                or line_cleaned.startswith("##### Expected Outcomes")
                or line_cleaned.startswith("# **Expected Outcomes**")
                or line_cleaned.startswith("## **Expected Outcomes**")
                or line_cleaned.startswith("### **Expected Outcomes**")
                or line_cleaned.startswith("#### **Expected Outcomes**")
                or line_cleaned.startswith("##### **Expected Outcomes**")
            ):
                current_section = "expected_outcomes"
            elif (
                line_cleaned.startswith("**Tags**")
                or line_cleaned.startswith("# Tags")
                or line_cleaned.startswith("## Tags")
                or line_cleaned.startswith("### Tags")
                or line_cleaned.startswith("#### Tags")
                or line_cleaned.startswith("##### Tags")
                or line_cleaned.startswith("# **Tags**")
                or line_cleaned.startswith("## **Tags**")
                or line_cleaned.startswith("### **Tags**")
                or line_cleaned.startswith("#### **Tags**")
                or line_cleaned.startswith("##### **Tags**")
            ):
                current_section = "tags"
            elif (
                line_cleaned.startswith("**manualSetupRequired**")
                or line_cleaned.startswith("# manualSetupRequired")
                or line_cleaned.startswith("## manualSetupRequired")
                or line_cleaned.startswith("### manualSetupRequired")
                or line_cleaned.startswith("#### manualSetupRequired")
                or line_cleaned.startswith("##### manualSetupRequired")
                or line_cleaned.startswith("# **manualSetupRequired**")
                or line_cleaned.startswith("## **manualSetupRequired**")
                or line_cleaned.startswith("### **manualSetupRequired**")
                or line_cleaned.startswith("#### **manualSetupRequired**")
                or line_cleaned.startswith("##### **manualSetupRequired**")
            ):
                current_section = "manualSetupRequired"
            else:
                # Append content to the current section
                if current_section == "problem_statement":
                    metadata["problem_statement"] += f" {line_cleaned}" if metadata["problem_statement"] else line_cleaned
                elif current_section in ["required_metadata", "expected_outcomes", "manualSetupRequired","tags"]:
                    if line_cleaned.startswith("- "):  # Handle list items
                        metadata[current_section].append(line_cleaned[2:].strip())

        # Clean up metadata
        metadata["problem_statement"] = metadata["problem_statement"].strip()
        return metadata


    def extract_metadata_new(self, notebook):
        """
        Extract metadata from the notebook's first cell, handling all possible heading formats including #, ##, ###, ####, #####.
        """
        notebook_cell = notebook['cells'][0]  # Assuming the first cell contains the metadata
        lines = notebook_cell["source"].split("\n")
        metadata = {
            "category": "",
            "subcategory": "",
            "tags": [],
            "screenshot": "",
            "complexity_category": [],
            "message": "",
            "problem_statement": "",
            "manualSetupRequired": []
        }
        if "**Required Metadata Before Executing the Code**" in lines:
            metadata["required_metadata_before_executing_the_code"] = ""

        

        current_section = None  # Tracks the active section being parsed

        for line in lines:
            line_cleaned = line.strip()
            # import pdb
            # pdb.set_trace()
            # Check for different heading possibilities and assign current_section
            if line_cleaned.startswith("**Category**") or any(line_cleaned.startswith(f"{h} Category") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "category"
                metadata["category"] = line_cleaned.split("-", 1)[-1].strip() if "-" in line_cleaned else ""
            elif line_cleaned.startswith("**Subcategory**") or any(line_cleaned.startswith(f"{h} Subcategory") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "subcategory"
                metadata["subcategory"] = line_cleaned.split("-", 1)[-1].strip() if "-" in line_cleaned else ""
            elif line_cleaned.startswith("**Tags Category**") or any(line_cleaned.startswith(f"{h} Tags Category") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "tags"
            elif line_cleaned.startswith("**Complexity Category**") or any(line_cleaned.startswith(f"{h} Complexity Category") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "complexity_category"
            elif line_cleaned.startswith("**Message**") or any(line_cleaned.startswith(f"{h} Message") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "message"
                metadata["message"] = line_cleaned.split("-", 1)[-1].strip() if "-" in line_cleaned else ""
            elif line_cleaned.startswith("**Problem Statement**") or any(line_cleaned.startswith(f"{h} Problem Statement") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "problem_statement"
            elif line_cleaned.startswith("**Screenshot**") or any(line_cleaned.startswith(f"{h} Screenshot") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "screenshot"
                metadata["screenshot"] = line_cleaned.split("-", 1)[-1].strip() if "-" in line_cleaned else ""
            elif line_cleaned.startswith("**Required Metadata Before Executing the Code**") or any(line_cleaned.startswith(f"{h} Required Metadata Before Executing the Code") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "required_metadata_before_executing_the_code"
            elif line_cleaned.startswith("**manualSetupRequired**") or any(line_cleaned.startswith(f"{h} manualSetupRequired") for h in ["#", "##", "###", "####", "#####"]):
                current_section = "manualSetupRequired"
            else:
                # Append content to the current section
                if current_section == "problem_statement":
                    metadata["problem_statement"] += f" {line_cleaned}" if metadata["problem_statement"] else line_cleaned
                elif current_section == "required_metadata_before_executing_the_code":
                    metadata["required_metadata_before_executing_the_code"] += f" {line_cleaned}" if metadata["required_metadata_before_executing_the_code"] else line_cleaned
                elif current_section in ["tags", "complexity_category",  "manualSetupRequired"]:
                    if line_cleaned.startswith("- "):
                        metadata[current_section].append(line_cleaned[2:].strip())

        # Clean up metadata
        metadata["problem_statement"] = metadata["problem_statement"].strip()
        return metadata

    def notebook_parser(self, content: str):
        nb_parsed_notebook = nbformat.reads(content, as_version=4)
        extracted_data = self.extract_messages(nb_parsed_notebook)
        messages = extracted_data["messages"]
        number_of_turns = extracted_data["number_of_turns"]  # Extract the count of user roles
        notebook_cell = nb_parsed_notebook['cells'][0]
        first_line = notebook_cell["source"].strip().split("\n")[1]
        #print(first_line)
        if  len(first_line) == 0:
            message = ("Extra linebreak after # Metadata")
            print(message)
            return {
            "status": "FAILED",
            "error_msg": message
            }

        if "**Topic**" in first_line or any(h + " Topic" in first_line for h in ["#", "##", "###", "####", "#####"]):
            metadata = self.extract_metadata(nb_parsed_notebook)
        elif "**Category**" in first_line or any(h + " Category" in first_line for h in ["#", "##", "###", "####", "#####"]):
            metadata = self.extract_metadata_new(nb_parsed_notebook)
        return {
            "status": "OK",
            "content_metadata": metadata,
            "messages": messages,
            "number_of_turns": number_of_turns,  # Include the number of turns in the output
        }


    def parse_notebooks(self, input_batch):
        input_batch = copy.deepcopy(input_batch)

        def parse_content(item):
            if item["content"]:
                return self.notebook_parser(item["content"])
            return {
                "status": "NONE",
            }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(parse_content, item): item
                for item in input_batch["items"]
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    item["parsed"] = future.result()
                except Exception as exc:
                    print(f"Generated an exception: {exc}")
                    item["parsed"] = {
                        "status": "FAILED",
                        "error_msg": str(exc),
                    }
        return input_batch

    def split_messages_into_turns(self, messages):
        turns = []
        current_role_steps = []
        if not messages:
            return {
                "status": "ERROR",
                "reason": "No messages were provided to turn splitter.",
            }

        current_role = messages[0]["role"]
        for message in messages:
            role = message["role"]
            if current_role != role:
                turns.append({"role": current_role, "steps": current_role_steps})
                current_role_steps = []
                current_role = role
            current_role_steps.append(
                {"type": message["type"], "content": message["content"]}
            )
        if current_role_steps:
            turns.append({"role": current_role, "steps": current_role_steps})

        grouped_turns = []
        for i in range(0, len(turns), 2):
            group = turns[i : i + 2]
            grouped_turns.append(group)
        return {"status": "OK", "turns": grouped_turns}

    def notebook_to_turns(self, notebook):
        parsed_notebook = {**self.notebook_parser(notebook)}
        turns = self.split_messages_into_turns(parsed_notebook["messages"])
        if turns["status"] == "OK":
            return turns["turns"]
        else:
            raise Exception("Bad notebook")