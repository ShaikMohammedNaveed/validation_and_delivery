import re
import sys
import os
import nbformat
from typing import List

class NotebookValidator:
    def __init__(self, content: str, file_path: str):
        self.content = content
        self.sections = {}
        self.errors = []
        self.file_names = set()
        self.class_names = set()
        self.file_path = file_path
        self.error_file = f"{os.path.splitext(file_path)[0]}_errors.txt"

    def parse_sections(self):
        """
        Parses the markdown content into sections based on headers.
        Assumes that headers are denoted by '#' symbols.
        """
        header_pattern = re.compile(r'^(#{1,6})\s(Conversation|Metadata)', re.MULTILINE)
        matches = list(header_pattern.finditer(self.content))
        loose_header_pattern = re.compile(r'^(#{1,6})\s{0,}\*?\*?(Metadata|Conversation)', re.MULTILINE)
        loose_matches = list(loose_header_pattern.finditer(self.content))
        loose_sections = set()
        fixed_sections = set()
        for i, match in enumerate(loose_matches):
            title = match.group(2).strip()
            if title not in loose_sections:
                loose_sections.add(title)
        for i, match in enumerate(matches):
            title = match.group(2).strip()
            if title not in fixed_sections:
                fixed_sections.add(title)
        if loose_sections - fixed_sections != set():
            self.errors.append(f"{loose_sections - fixed_sections} is not formatted properly, Please format header to '# Metadata' or '# Conversation' like format.")
        # print("loose sections:",loose_sections)
        # print("fixed sections",fixed_sections)
        sections = {}
        for i, match in enumerate(loose_matches):
            level = len(match.group(1))
            title = match.group(2).strip()
            start = match.end()
            end = loose_matches[i + 1].start() if i + 1 < len(loose_matches) else len(self.content)
            if title in sections:
                self.errors.append(f"Duplicate section detected: '{title}'. Each section should appear only once.")
            sections[title] = {
                'level': level,
                'content': self.content[start:end].strip()
            }
        self.sections = sections

    def validate_metadata(self):
        """
        Validates the metadata section format and its content dynamically.
        Ensures headers are correctly formatted, contain values, and special cases are handled.
        """
        metadata_title = 'Metadata'
        if metadata_title not in self.sections:
            self.errors.append("Missing '# Metadata' section.")
            return

        metadata_content = self.sections[metadata_title]['content']

        manual_pattern = re.compile(r'[#|\s]{0,4}\*\*manualSetupRequired\*\*\s*-\s*(\S+)')
        manual_match = manual_pattern.search(metadata_content)
        if manual_match:
            manual_value = manual_match.group(1).strip()
            if manual_value not in ["True", "False"]:
                self.errors.append("**manualSetupRequired** must have a value of either 'True' or 'False'.")
        else:
            self.errors.append("Missing '**manualSetupRequired**' or incorrect formatting.")

    def validate_conversation(self):
        """
        Validates the conversation format ensuring:
        - User and Assistant blocks alternate correctly.
        - No consecutive '**User**' blocks without an '**Assistant**' in between.
        - Clarification Question (if exists) must be followed by User or nothing.
        - Blueprint → Implementation plan → Scaffolding code → Code appear in correct order.
        - All 4 Assistant blocks must exist in sequence, followed by a User or nothing.
        - Displays the cell number where errors occur.
        """
        user_pattern = re.compile(r'^#{0,4}\s?\*\*User\*\*$')
        assistant_pattern = re.compile(r'^#{0,4}\s?\*\*Assistant\*\*$')
        clarification_pattern = re.compile(r'#{0,4}\s?\*\*Clarification Question\*\*')

        subheading_order = [
            ("Blueprint", re.compile(r'\*\*Blueprint\*\*', re.IGNORECASE)),
            ("Implementation plan", re.compile(r'\*\*Implementation plan\*\*', re.IGNORECASE)),
            ("Scaffolding code", re.compile(r'\*\*Scaffolding code\*\*', re.IGNORECASE)),
            ("Code", re.compile(r'\*\*Code\*\*', re.IGNORECASE))
        ]

        # Check if conversation exists in notebook
        conversation_title = "Conversation"
        if conversation_title not in self.sections and "#Conversation" not in self.sections:
            self.errors = []
            self.errors.append("No conversation header found, please add/format a '# Conversation' section to the notebook.")
            # self.report_errors()
            conversation_title = "**Conversation**"
            return
        conversation_content = self.sections[conversation_title]["content"]
        lines = [line.strip() for line in conversation_content.split("\n")]

        blocks = []
        current_role = None
        current_text = []
        cell_number = 1
        header_pattern = re.compile(r'^#{0,4}\s?\*\*.+\*\*$')

        raw_lines = conversation_content.split("\n")
        for i in range(len(raw_lines)-1):
            # If this line is a header…
            if header_pattern.match(raw_lines[i].strip()):
                # …and the very next line is blank, then flag it.
                if i > 3 and raw_lines[i-2].strip() == "" and raw_lines[i-2].strip() in ['**Assistant**', '**Blueprint**', '**Implementation plan**', '**Scaffolding code**', '**Code**']:
                    self.errors.append(
                        f"❌ Line {i+2}: Please remove extra newline before '{raw_lines[i].strip()}' who has next header/content like '{raw_lines[i+2].strip()[:15]}', if you cannot see any extra newline please check the previous cell's last line."
                    )
                elif i <= 2 and raw_lines[i-1].strip() == "":
                    self.errors.append(
                        f"❌ Line {i+2}: Extra blank line detected before header '{raw_lines[i].strip()}'. Please remove extra newline space, if you cannot see any extra newline please check the previous cell's last line."
                    )
        for line in lines:
            if user_pattern.match(line):
                if current_role:
                    blocks.append((current_role, "\n".join(current_text).strip(), cell_number))
                    cell_number += 1  # Increment cell count
                current_role = "User"
                current_text = []
            elif assistant_pattern.match(line):
                if current_role:
                    blocks.append((current_role, "\n".join(current_text).strip(), cell_number))
                    cell_number += 1  # Increment cell count
                current_role = "Assistant"
                current_text = []
            else:
                current_text.append(line)
        if current_role:
            blocks.append((current_role, "\n".join(current_text).strip(), cell_number))

        # **Validating conversation structure**
        last_role = None
        expecting_user_after_clarification = False
        subheading_index = 0
        assistant_subheadings = set()  # To track all expected Assistant subheadings

        for i, (role, text, cell_num) in enumerate(blocks):
            # 1. Ensure no consecutive User blocks
            if last_role == "User" and role == "User":
                self.errors.append(f"❌ Cell {cell_num + 2}: Consecutive '**User**' blocks found without an '**Assistant**' response in between.")
            if last_role == "Assistant" and role == "Assistant" and not any(subheading_regex.search(text) for _, subheading_regex in subheading_order[1:]):
                # if not expecting_subheading_sequence:
                self.errors.append(f"❌ Cell {cell_num + 2}: Consecutive '**Assistant**' blocks found without a '**User**' in between.")

            if role == "User":
                if expecting_user_after_clarification:
                    expecting_user_after_clarification = False  # Clarification Question is correctly followed by User
                if text.strip() == "":
                    self.errors.append(f"❌ Cell {cell_num + 2}: User block is empty: missing user prompt.")
                if subheading_index > 1 and subheading_index < 4:
                    self.errors.append(f"❌ Cell {cell_num + 2}: Expected '**{subheading_order[subheading_index][0]}**' but found '**User**' response.")
                assistant_subheadings.clear()   
                subheading_index = 0
                
            elif role == "Assistant":
                if clarification_pattern.search(text):
                    expecting_user_after_clarification  = True  # Clarification Question must be followed by User or nothing
                    subheading_index = 0  # Reset subheading index after Clarification Question
                else:
                    for subheading_name, subheading_regex in subheading_order:
                        if subheading_regex.search(text):
                            if subheading_name == "Blueprint" and subheading_index > 0:
                                self.errors.append(f"❌ Cell {cell_num + 2}: Please check if you have missed user prompt before Blueprint.")
                            if subheading_name in assistant_subheadings and subheading_index <= 4 and subheading_name != "Blueprint":
                                self.errors.append(f"❌ Cell {cell_num + 2}: Duplicate '**{subheading_name}**' response found.")
                                continue
                            assistant_subheadings.add(subheading_name)  # Mark subheading as found
                            if subheading_index == 0 and subheading_name != "Blueprint":
                                self.errors.append(f"❌ Cell {cell_num + 2}: Expected '**Blueprint**' but found '**{subheading_name}**' first.")

                            elif subheading_name != subheading_order[subheading_index%4][0]:
                                self.errors.append(f"❌ Cell {cell_num + 2}: Expected '**{subheading_order[subheading_index][0]}**' but found '**{subheading_name}**' out of order.")
                            subheading_index += 1

                            if subheading_name == "Blueprint":
                                self.validate_blueprint(text, cell_num + 2)
                            elif subheading_name.lower() == "Implementation Plan".lower():
                                self.validate_implementation_plan(text, cell_num + 2)
                            elif subheading_name == "Scaffolding code":
                                self.validate_scaffolding_code(text, cell_num + 2)
                            elif subheading_name == "Code":
                                self.validate_code(text, cell_num + 2)
                    if subheading_index == 0:
                        self.errors.append(f"❌ Cell {cell_num + 2}: Expected one of '**Blueprint**, **Implementation plan**, **Scaffolding code**, **Code**' but found none, or did you forgot mention **Clarification Question** header?")
            last_role = role

        # Check if all 4 Assistant subheadings are present in sequence
        missing_subheadings = [subheading[0] for subheading in subheading_order if subheading[0] not in assistant_subheadings]
        if missing_subheadings:
            self.errors.append(f"❌ Missing required Assistant responses: {', '.join(missing_subheadings)}. Expected all 4 in sequence.")
        # If conversation ended but was expecting a User after Clarification Question
        if expecting_user_after_clarification:
            self.errors.append("❌ Conversation ended, but a '**User**' response was expected after Clarification Question.")

    def validate_blueprint(self, text, cell_num):
        """
        Validates 'Blueprint' format in a notebook cell, checking:
        1) The presence of bold headers (other than 'Assistant'/'Blueprint').
        2) Each header's section must contain at least one Name (in bold),
            and for each Name, at least one (bold) What and (bold) Why.
        3) Any line that appears to be a header but is not wrapped in **...**
            triggers an error (to ensure 'Overview' is always bolded).
        """
        # Only proceed if 'Blueprint' is mentioned
        if "Blueprint" not in text:
            return
        
        lines = text.split("\n")
        if len(lines) < 3:
            self.errors.append(f"❌ Cell {cell_num}: Incomplete Blueprint section.")
            return

        # ----------------------------------------------------------------
        # STEP 1: Identify bold headers using a strict regex
        #         but skip "Assistant" and "Blueprint" themselves.
        # ----------------------------------------------------------------
        header_pattern = re.compile(r'^\*\*(.+)\*\*\s*$')
        headers = []  # list of (header_text, line_index) for recognized bold headers
        for i, raw_line in enumerate(lines):
            line_stripped = raw_line.strip()
            match = header_pattern.match(line_stripped)
            if match:
                found_header = match.group(1).strip()  # e.g. "Overview"
                if found_header not in ["Assistant", "Blueprint"]:
                    headers.append((found_header, i))

        # If no valid headers found at all, raise an error
        if not headers:
            self.errors.append(
                f"❌ Cell {cell_num}: No valid bold headers found (other than '**Assistant**' and '**Blueprint**')."
            )
            return

        # ----------------------------------------------------------------
        # STEP 2 (Optional): Check for lines that look like "headers" but
        #         are NOT in bold. This is to catch lines like "Overview"
        #         without "**Overview**".
        #
        #         We'll define a "possible header" as:
        #           - Non-empty line
        #           - Not a bullet or numbered line
        #           - Not a Name/What/Why line
        #           - Doesn't contain a colon (i.e., not "Name: something")
        #           - Not matched by the bold-header pattern
        #
        #         If you only want to check *known* keywords like "Overview"
        #         or "Content Requirements", adapt this logic.
        # ----------------------------------------------------------------

        # Regex that might qualify a bullet line or Name/What/Why line
        bullet_pattern = re.compile(r'^\s*(?:-|\d+\.)')
        name_loose_regex = re.compile(r'\s*\*?\*?Name\*?\*?\s*:', re.IGNORECASE)
        what_loose_regex = re.compile(r'\s*\*?\*?What\*?\*?\s*:', re.IGNORECASE)
        why_loose_regex  = re.compile(r'\s*\*?\*?Why\*?\*?\s*:', re.IGNORECASE)

        for raw_line in lines:
            line_stripped = raw_line.strip()

            # skip empty lines
            if not line_stripped:
                continue

            # skip the lines that *are* recognized as bold headers
            if header_pattern.match(line_stripped):
                continue

            # skip bullet/numbered lines or lines with colon (Name:..., etc.)
            if bullet_pattern.match(line_stripped):
                continue
            if ":" in line_stripped:
                # Typically, lines with colon are "Name: / What: / Why: / or normal text with colon"
                continue

            # If it has "Name" or "What" or "Why", skip
            # (Alternatively, check if name_loose_regex.match(line_stripped) etc.)
            # but if "Name"/"What"/"Why" has no colon, this is weird but let's skip.
            if any(keyword in line_stripped for keyword in ["Name", "What", "Why"]):
                continue

            # Now we have a line that:
            #  - is not empty
            #  - is not recognized as a bullet or name line
            #  - does not match the bold header pattern
            #  - doesn't have a colon
            # => It's suspiciously a "plain text" line that could be a header
            #    but is not in bold. So let's produce an error:
            if len(line_stripped) < 15:
                self.errors.append(
                        f"❌ Cell {cell_num}: Line '{line_stripped}' is a possible header but is not bold formatted (must be `**...**`)."
                )

        # ----------------------------------------------------------------
        # STEP 3: Now parse each header's section for Name / What / Why
        # ----------------------------------------------------------------

        # Helper function: slice the lines belonging to each header's section
        def get_section_lines(h_idx, all_headers, all_lines):
            _, start_line_idx = all_headers[h_idx]
            if h_idx == len(all_headers) - 1:
                return all_lines[start_line_idx + 1 :]
            _, next_line_idx = all_headers[h_idx + 1]
            return all_lines[start_line_idx + 1 : next_line_idx]

        # Strict/bold patterns for Name, What, Why
        name_strict_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*(\*\*Name\*\*)\s*:\s*(.+)$')
        what_strict_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*(\*\*What\*\*)\s*:\s*(.+)$')
        why_strict_regex  = re.compile(r'^\s*(?:-|\d+\.)?\s*(\*\*Why\*\*)\s*:\s*(.+)$')

        # Re-use the "loose" patterns for detecting presence
        # but allow optional bullet or numbering
        name_loose_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(Name)\*?\*?\s*:\s*(.+)$', re.IGNORECASE)
        what_loose_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(What)\*?\*?\s*:\s*(.+)$', re.IGNORECASE)
        why_loose_regex  = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(Why)\*?\*?\s*:\s*(.+)$', re.IGNORECASE)

        # For each header, check that there's at least one Name,
        # and each Name has a What and Why in its sub-block
        for h_index, (header_text, _) in enumerate(headers):
            section_lines = get_section_lines(h_index, headers, lines)

            # gather all lines in this section that contain "Name"
            name_line_indices = []
            for idx_in_section, raw_line in enumerate(section_lines):
                if name_loose_regex.match(raw_line.strip()):
                    name_line_indices.append(idx_in_section)

            if not name_line_indices:
                self.errors.append(
                    f"❌ Cell {cell_num}: Section '**{header_text}**' does not contain/ not formatted as '**Name**:'"
                )
                # move to next header
                continue

            # For each name line found
            for idx_local, name_line_idx in enumerate(name_line_indices):
                name_line_stripped = section_lines[name_line_idx].strip()

                # Check if it's in strict bold
                if not name_strict_regex.match(name_line_stripped):
                    self.errors.append(
                        f"❌ Cell {cell_num}: 'Name' is not in bold or not formatted correctly: '{name_line_stripped}'."
                    )

                # sub-block for searching "What"/"Why": from after this name line until next name or end of section
                if (idx_local + 1) < len(name_line_indices):
                    next_name_idx = name_line_indices[idx_local + 1]
                    sub_block = section_lines[name_line_idx + 1 : next_name_idx]
                else:
                    sub_block = section_lines[name_line_idx + 1 :]

                # Check at least one "What"
                has_what = False
                for ln in sub_block:
                    ln_stripped = ln.strip()
                    if what_loose_regex.match(ln_stripped):
                        has_what = True
                        # check bold
                        if not what_strict_regex.match(ln_stripped):
                            self.errors.append(
                                f"❌ Cell {cell_num}: 'What' is not in bold or not formatted correctly: '{ln_stripped}'."
                            )
                if not has_what:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Missing 'What' entry after 'Name' line: '{name_line_stripped}'."
                    )

                # Check at least one "Why"
                has_why = False
                for ln in sub_block:
                    ln_stripped = ln.strip()
                    if why_loose_regex.match(ln_stripped):
                        has_why = True
                        # check bold
                        if not why_strict_regex.match(ln_stripped):
                            self.errors.append(
                                f"❌ Cell {cell_num}: 'Why' is not in bold or not formatted correctly: '{ln_stripped}'."
                            )
                if not has_why:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Missing 'Why' entry after 'Name' line: '{name_line_stripped}'."
                    )

    def validate_implementation_plan(self, text, cell_num):
        """
        Validates the content of a notebook cell containing an 'Implementation plan', checking:

        1) "Implementation plan" mention.
        2) Bold headers (excluding 'Assistant'/'Implementation plan') recognized.
        3) Each header block must contain at least one '**Name**:' line.
        4) For each '**Name**:' line, the sub-block must include '**What**:', '**Why**:', and '**Step**:' fields.
        - Each of these must not be empty. We allow either same-line text or sub-bullets.
        5) Optional fields like '**File**:', '**Class**:', '**Method**:' must:
        - Be bolded (no "File:" without "** **").
        - Not be empty; again, can have same-line text or sub-bullets.
        6) Filenames found in '**File**:' get added to self.file_names.
        7) `.cls` class names found in '**Class**:' get added to self.class_names.
        8) Unbolded lines that might be headers or fields produce errors.
        """

        # Quick exit if 'Implementation plan' not present
        if "Implementation plan".lower() not in text.lower():
            return

        lines = text.split("\n")
        if len(lines) < 3:
            self.errors.append(f"❌ Cell {cell_num}: Incomplete Implementation plan section.")
            return

        # -------------------------------------------------------------
        # 1) Identify bold headers (excluding "Assistant"/"Implementation plan")
        # -------------------------------------------------------------
        header_pattern = re.compile(r'^\*\*(.+)\*\*\s*$')
        headers = []
        for i, raw_line in enumerate(lines):
            line_stripped = raw_line.strip()
            match = header_pattern.match(line_stripped)
            if match:
                found_header = match.group(1).strip()
                if found_header.lower() not in ["assistant", "implementation plan"]:
                    headers.append((found_header, i))

        if not headers:
            self.errors.append(
                f"❌ Cell {cell_num}: No valid bold headers found (other than '**Assistant**'/'**Implementation plan**')."
            )
            return

        # -------------------------------------------------------------
        # 2) Check for suspicious lines that might be un-bolded headers
        # -------------------------------------------------------------
        bullet_pattern = re.compile(r'^\s*(?:-|\d+\.)')
        for raw_line in lines:
            line_stripped = raw_line.strip()
            if not line_stripped:
                continue
            if header_pattern.match(line_stripped):
                continue
            if bullet_pattern.match(line_stripped):
                continue
            if ":" in line_stripped:
                continue
            if any(keyword in line_stripped for keyword in ["Name", "What", "Why", "Step"]):
                continue
            # If we get here, it looks like it might be a header but isn't bolded
            if len(line_stripped) < 15:
                self.errors.append(
                        f"❌ Cell {cell_num}: Line '{line_stripped}' is a possible header but is not bold formatted (must be `**...**`)."
                )

        # -------------------------------------------------------------
        # 3) Helper to slice sub-block lines between headers
        # -------------------------------------------------------------
        def get_section_lines(h_idx, all_headers, all_lines):
            _, start_line_idx = all_headers[h_idx]
            if h_idx == len(all_headers) - 1:
                return all_lines[start_line_idx + 1:]
            _, next_line_idx = all_headers[h_idx + 1]
            return all_lines[start_line_idx + 1 : next_line_idx]

        # -------------------------------------------------------------
        # 4) Regex definitions
        # -------------------------------------------------------------
        name_strict_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*\*Name\*\*:\s*(.*)$')
        what_strict_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*\*What\*\*:\s*(.*)$')
        why_strict_regex  = re.compile(r'^\s*(?:-|\d+\.)?\s*\*\*Why\*\*:\s*(.*)$')
        step_strict_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*\*Step\*\*:\s*(.*)$')

        name_loose_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(Name)\*?\*?:\s*(.*)$', re.IGNORECASE)
        what_loose_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(What)\*?\*?:\s*(.*)$', re.IGNORECASE)
        why_loose_regex  = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(Why)\*?\*?:\s*(.*)$', re.IGNORECASE)
        step_loose_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?(Step)\*?\*?:\s*(.*)$', re.IGNORECASE)

        # Optional bold fields pattern, e.g. '**File**:', '**Class**:', '**Method**:', etc.
        optional_bold_field_regex = re.compile(r'^\s*(?:-|\d+\.)\s*\*\*([\w]+)\*\*:\s*$')
        # Non-bold optional fields pattern (e.g. '- File: ...'), used to detect missing bold
        optional_non_bold_regex = re.compile(r'^\s*(?:-|\d+\.)?\s*\*?\*?([\w]+)\*?\*?:\s*$')

        # -------------------------------------------------------------
        # 5) Helper to gather multiline content for a field
        # -------------------------------------------------------------
        def get_multiline_field_content(idx_in_subblock, sub_block, start_patterns):
            collected_bullets = []
            for i2 in range(idx_in_subblock + 1, len(sub_block)):
                next_line_stripped = sub_block[i2].strip()
                # Check if next_line_stripped starts a new field
                if any(pat.match(next_line_stripped) for pat in start_patterns):
                    break
                if bullet_pattern.match(next_line_stripped):
                    content_after_bullet = bullet_pattern.sub("", next_line_stripped).strip()
                    if content_after_bullet:
                        collected_bullets.append(content_after_bullet)
                else:
                    if next_line_stripped:
                        collected_bullets.append(next_line_stripped)
            return "\n".join(collected_bullets).strip()

        all_field_start_patterns = [
            name_loose_regex,
            what_loose_regex,
            why_loose_regex,
            step_loose_regex,
            optional_bold_field_regex,
            optional_non_bold_regex,
        ]

        # ADDED: define mappings for plural -> singular
        plurals_to_singulars = {
            "Files":   "File",
            "Steps":   "Step",
            "Methods": "Method",
            "Classes": "Class"
        }

        # -------------------------------------------------------------
        # 6) Parse each header
        # -------------------------------------------------------------
        for h_index, (header_text, _) in enumerate(headers):
            section_lines = get_section_lines(h_index, headers, lines)

            # (a) Collect all 'Name' lines
            name_line_indices = []
            for idx_in_section, raw_line in enumerate(section_lines):
                if name_loose_regex.match(raw_line.strip()):
                    name_line_indices.append(idx_in_section)

            if not name_line_indices:
                self.errors.append(
                    f"❌ Cell {cell_num}: Section '**{header_text}**' does not contain/ not formatted as '**Name**:'"
                )
                continue

            # (b) For each 'Name' line, check sub-block for required fields
            for idx_local, name_line_idx in enumerate(name_line_indices):
                name_line_stripped = section_lines[name_line_idx].strip()

                # Strict check for Name
                name_match_strict = name_strict_regex.match(name_line_stripped)
                if not name_match_strict:
                    self.errors.append(
                        f"❌ Cell {cell_num}: 'Name' is not bold or incorrectly formatted: '{name_line_stripped}'."
                    )
                else:
                    name_content = name_match_strict.group(1).strip()
                    if not name_content:
                        self.errors.append(
                            f"❌ Cell {cell_num}: 'Name' field is empty (no text after the colon)."
                        )

                # Sub-block from after the Name line up to the next Name or end
                if (idx_local + 1) < len(name_line_indices):
                    next_name_idx = name_line_indices[idx_local + 1]
                    sub_block = section_lines[name_line_idx + 1 : next_name_idx]
                else:
                    sub_block = section_lines[name_line_idx + 1 :]

                has_what = False
                has_why  = False
                has_step = False

                # (c) Scan the sub-block line by line
                i_sub = 0
                while i_sub < len(sub_block):
                    ln_stripped = sub_block[i_sub].strip()

                    # --- WHAT ---
                    match_what_loose = what_loose_regex.match(ln_stripped)
                    if match_what_loose:
                        has_what = True
                        match_what_strict = what_strict_regex.match(ln_stripped)
                        if not match_what_strict:
                            self.errors.append(
                                f"❌ Cell {cell_num}: 'What' is not bold or incorrectly formatted: '{ln_stripped}'."
                            )
                        else:
                            content = match_what_strict.group(1).strip()
                            if not content:
                                multiline = get_multiline_field_content(i_sub, sub_block, all_field_start_patterns)
                                if not multiline:
                                    self.errors.append(
                                        f"❌ Cell {cell_num}: 'What' field is empty (no text inline or in bullets)."
                                    )
                        i_sub += 1
                        continue

                    # --- WHY ---
                    match_why_loose = why_loose_regex.match(ln_stripped)
                    if match_why_loose:
                        has_why = True
                        match_why_strict = why_strict_regex.match(ln_stripped)
                        if not match_why_strict:
                            self.errors.append(
                                f"❌ Cell {cell_num}: 'Why' is not bold or incorrectly formatted: '{ln_stripped}'."
                            )
                        else:
                            content = match_why_strict.group(1).strip()
                            if not content:
                                multiline = get_multiline_field_content(i_sub, sub_block, all_field_start_patterns)
                                if not multiline:
                                    self.errors.append(
                                        f"❌ Cell {cell_num}: 'Why' field is empty (no text inline or in bullets)."
                                    )
                        i_sub += 1
                        continue

                    # --- STEP ---
                    match_step_loose = step_loose_regex.match(ln_stripped)
                    if match_step_loose:
                        has_step = True
                        match_step_strict = step_strict_regex.match(ln_stripped)
                        if not match_step_strict:
                            self.errors.append(
                                f"❌ Cell {cell_num}: 'Step' is not bold or incorrectly formatted: '{ln_stripped}'."
                            )
                        else:
                            content = match_step_strict.group(1).strip()
                            if not content:
                                multiline = get_multiline_field_content(i_sub, sub_block, all_field_start_patterns)
                                if not multiline:
                                    self.errors.append(
                                        f"❌ Cell {cell_num}: 'Step' field is empty (no text inline or in bullets)."
                                    )
                        i_sub += 1
                        continue

                    match_opt_bold = optional_bold_field_regex.match(ln_stripped)
                    if match_opt_bold:
                        field_name = match_opt_bold.group(1).strip()
                        # ADDED: if it's a plural, provide the correction
                        if field_name in plurals_to_singulars:
                            corrected = plurals_to_singulars[field_name]
                            self.errors.append(
                                f"❌ Cell {cell_num}: You wrote '{field_name}'. Please correct it to '{corrected}'."
                            )
                        # There's no group(2) in optional_bold_field_regex, so field_val is empty
                        field_val = ""
                        multiline = get_multiline_field_content(i_sub, sub_block, all_field_start_patterns)
                        if not multiline:
                            self.errors.append(
                                f"❌ Cell {cell_num}: Field '{field_name}' is empty (no text inline or in bullets)."
                            )

                        # Special handling for 'File' and 'Class'
                        if field_name.lower() == "file":
                            for bullet_line in multiline.split("\n"):
                                self.file_names.add(bullet_line.strip())
                        elif field_name.lower() == "class":
                            for bullet_line in multiline.split("\n"):
                                if bullet_line:
                                    self.class_names.add(bullet_line.strip())

                        i_sub += 1
                        continue

                    match_opt_non_bold = optional_non_bold_regex.match(ln_stripped)
                    if match_opt_non_bold:
                        possible_field_name = match_opt_non_bold.group(1).strip()

                        # ADDED: if it's a plural, provide the correction
                        if possible_field_name in plurals_to_singulars:
                            corrected = plurals_to_singulars[possible_field_name]
                            self.errors.append(
                                f"❌ Cell {cell_num}: You wrote '{possible_field_name}'. Please correct it to '{corrected}'."
                            )

                        self.errors.append(
                            f"❌ Cell {cell_num}: Field '{possible_field_name}' is not bold-formatted. Must be '**{possible_field_name}**:'."
                        )
                        i_sub += 1
                        continue

                    i_sub += 1

                # (d) After scanning sub-block, ensure we found required fields
                if not has_what:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Missing 'What' entry after 'Name' line: '{name_line_stripped}'."
                    )
                if not has_why:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Missing 'Why' entry after 'Name' line: '{name_line_stripped}'."
                    )

    def _validate_code_lines(
        self,
        lines,
        cell_num,
        file_line_regex,
        code_fence_start_regex,
        code_fence_end,
        code_block_type_map,
        scaf_required=False
    ):
        """
        Validates code lines in a conversation or scaffolding code block.

        If `scaf_required=True`, we specifically require a filename 
        that ends with '.scaf' (e.g. `MyFile.html.scaf`).

        Changes:
        - If user typed a line with backticks but missing '.scaf', 
        raise:  "'.scaf' is missing in file name 'XYZ'."
        - If the line doesn't match the backtick pattern at all,
        raise:  "File name was not formatted properly."

        The rest of the logic remains the same for checking code fences (```lang) 
        and ensuring they match the file extension.
        """

        i = 0
        n = len(lines)

        # We define a "looser" file format regex that checks if it’s in backticks 
        # but might not have `.scaf`.
        #  e.g.: `OpportunityManager.cls`  or  `loginForm.html`  etc.
        loose_file_line_regex = re.compile(r'^(?:\*\*)?`([^`]+)`(?:\*\*)?$')

        # For convenience, let’s also define a sub-regex to see if “.scaf” is in that name
        scaf_suffix_regex = re.compile(r'\.scaf\s*$', re.IGNORECASE)

        while i < n:
            line = lines[i].strip()

            # 1) Try matching the stricter file_line_regex first:
            file_match = file_line_regex.match(line)
            if file_match:
                # This means the line is already in the correct format 
                # and includes .scaf if scaf_required was used 
                # (because file_line_regex is presumably something like:
                #      r'^(?:\*\*)?`([^`]+?\.(\w+)\.scaf)`(?:\*\*)?$'
                # for scaffolding code).
                #
                # Move on to code fence checks below.
                full_file_name = file_match.group(1)

                # If needed, ensure the extension is extracted
                extension = file_match.group(2).lower() if len(file_match.groups()) > 1 else ""

                # Next lines look for a code fence:
                j = i + 1
                while j < n and not lines[j].strip():
                    j += 1

                if j >= n:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Expected a code fence (```lang) after file '{full_file_name}', but found none."
                    )
                    break

                fence_line = lines[j].strip()
                fence_match = code_fence_start_regex.match(fence_line)
                if not fence_match:
                    # Possibly partial backticks or incorrect format
                    if fence_line.count("`") < 3:
                        self.errors.append(
                            f"❌ Cell {cell_num}: Found partial backticks '{fence_line}'. Triple backticks (```lang) are required."
                        )
                    elif fence_line.startswith("``` "):
                        self.errors.append(
                            f"❌ Cell {cell_num}: Please remove extra space after the triple backticks. Found '{fence_line}'."
                        )
                    else:
                        self.errors.append(
                            f"❌ Cell {cell_num}: Expected a code fence (```lang) after file '{full_file_name}', but found '{fence_line}'."
                        )
                    i = j
                    continue

                declared_lang = fence_match.group(1).lower()
                valid_langs = code_block_type_map.get(extension, [])
                if valid_langs:
                    if declared_lang not in [v.lower() for v in valid_langs]:
                        self.errors.append(
                            f"❌ Cell {cell_num}: File extension '.{extension}' expects code fence language "
                            f"{valid_langs}, but found '{declared_lang}'."
                        )
                else:
                    if not scaf_required and extension.lower() == 'scaf':
                        self.errors.append(
                            f"❌ Cell {cell_num}: Found '.scaf' extension in '{full_file_name}' but this is not a scaffolding code block."
                        )
                    else:    
                    # Unrecognized extension for this code block map
                        self.errors.append(
                            f"❌ Cell {cell_num}: Unrecognized file extension '.{extension}' in '{full_file_name}'. "
                            "Cannot validate code block language."
                        )

                # Find the closing fence
                k = j + 1
                found_closing_fence = False
                while k < n:
                    if lines[k].strip() == code_fence_end:
                        found_closing_fence = True
                        break
                    k += 1

                if not found_closing_fence:
                    self.errors.append(
                        f"❌ Cell {cell_num}: Code block for file '{full_file_name}' is not closed with ```."
                    )
                    i = k
                    continue

                # If found, move i beyond the fence
                i = k + 1

            else:
                # 2) If we did NOT match the "official" file_line_regex, 
                #    check if they typed a line in backticks with missing .scaf

                # (A) Check if line is in backticks at all:
                loose_match = loose_file_line_regex.match(line)
                if scaf_required and loose_match:
                    # They typed something in backticks but not matching ".scaf"
                    loose_file_name = loose_match.group(1).strip()
                    # If it truly has no ".scaf"
                    if not scaf_suffix_regex.search(loose_file_name):
                        # => error: ".scaf is missing in file name"
                        self.errors.append(
                            f"❌ Cell {cell_num}: '.scaf' is missing in file name '{loose_file_name}'."
                        )
                    else:
                        # If they typed backticks but still didn't match the main pattern,
                        # it might be some other formatting issue.
                        self.errors.append(
                            f"❌ Cell {cell_num}: File name was not formatted properly: '{loose_file_name}'. "
                            "Expected e.g. `MyFile.html.scaf`."
                        )

                    # Move forward by 1 line only
                    i += 1

                elif loose_match:
                    # This is a "non-scaffolding" scenario 
                    # (scaf_required is False) but we didn't match 
                    # the official file_line_regex. Possibly missing extension or 
                    # something else. We could note a generic message.
                    file_name = loose_match.group(1).strip()
                    self.errors.append(
                        f"❌ Cell {cell_num}: File name was not formatted properly: '{file_name}'. "
                    )
                    i += 1

                else:
                    # (B) Not in backticks at all, or partial backticks
                    #     Could be code fences or partial backticks or plain text
                    if line.count("`") in [1,2]:
                        self.errors.append(
                            f"❌ Cell {cell_num}: Found partial backticks '{line}'. Triple backticks (```lang) are required."
                        )
                    elif line.startswith("```"):
                        # It's a code fence but there's no preceding filename
                        if scaf_required:
                            self.errors.append(
                                f"❌ Cell {cell_num}: Expected a '.scaf' file name before starting a code fence, but found none. '{line}'"
                            )
                        else:
                            self.errors.append(
                                f"❌ Cell {cell_num}: Found a code fence with no preceding filename. '{line}'"
                            )
                    # else no specific pattern => just continue scanning
                    i += 1

    def validate_scaffolding_code(self, text, cell_num):
        """
        Validates a 'Scaffolding Code' cell. Specifically:
          - Looks for lines with `.scaf` at the end.
          - Ensures each code fence has the correct language mapping based on extension.
        """

        # If you ONLY want to validate if text has "Scaffolding Code", do:
        if "Scaffolding Code" not in text:
            return

        # Common code fence config
        code_fence_start_regex = re.compile(r'^```(\w+)\s*$')
        code_fence_end = "```"

        # Code block type map
        code_block_type_map = {
            "cls":     ["apex"],       # .cls => ```apex
            "trigger": ["apex"],       # .trigger => ```apex
            "js":      ["javascript", "js"],
            "css":     ["css"],
            "html":    ["html"],
            "page":    ["html"],
            "xml":     ["xml"],
            "json":    ["json"],
            "java":    ["java"],
        }

        # Regex specifically enforcing `.scaf` at the end
        # Group(1) => "fileName.ext.scaf"
        # Group(2) => "ext"
        file_line_regex = re.compile(
            r'^(?:\*\*)?`([^`]+?\.(\w+)\.scaf)`(?:\*\*)?$'
        )

        lines = text.split("\n")

        # Call the helper
        self._validate_code_lines(
            lines=lines,
            cell_num=cell_num,
            file_line_regex=file_line_regex,
            code_fence_start_regex=code_fence_start_regex,
            code_fence_end=code_fence_end,
            code_block_type_map=code_block_type_map,
            scaf_required=True
        )

    def validate_code(self, text, cell_num):
        """
        Validates general code blocks. Does NOT require .scaf at the end.
        Everything else is the same (filename => extension => code fence language).
        """

        code_fence_start_regex = re.compile(r'^```(\w+)\s*$')
        code_fence_end = "```"

        code_block_type_map = {
            "cls":     ["apex"],
            "trigger": ["apex"],
            "js":      ["javascript", "js"],
            "css":     ["css"],
            "html":    ["html"],
            "xml":     ["xml"],
            "json":    ["json"],
            "java":    ["java"],
        }

        # This regex does NOT require .scaf
        # e.g. `OpportunityManager.cls`
        # Group(1) => "OpportunityManager.cls"
        # Group(2) => "cls"
        file_line_regex = re.compile(
            r'^(?:\*\*)?`([^`]+?\.(\w+))`(?:\*\*)?$'
        )

        lines = text.split("\n")

        self._validate_code_lines(
            lines=lines,
            cell_num=cell_num,
            file_line_regex=file_line_regex,
            code_fence_start_regex=code_fence_start_regex,
            code_fence_end=code_fence_end,
            code_block_type_map=code_block_type_map,
            scaf_required=False  # not enforcing .scaf
        )

    def validate_structure(self):
        """
        Runs all validation methods.
        """

        self.parse_sections()
        self.validate_metadata()
        self.validate_conversation()

    def report_errors(self):
        """Reports and stores all collected validation errors."""
        if not self.errors:
            print(f"✅ {self.file_path} is valid.")
            print('-'*40)
            if os.path.exists(self.error_file):
                os.remove(self.error_file)
        else:
            print(f"❌ Errors in {self.file_path}:")
            with open(self.error_file, 'w', encoding='utf-8') as f:
                for error in self.errors:
                    print(f"- {error}\n" + "-"*40)
                    f.write(error + "\n")
                    f.write("="*50 + "\n")

    def validate(self):
        """
        Executes the validation process and reports errors.
        """
        self.validate_structure()
        self.report_errors()

def extract_markdown_from_ipynb(filepath: str) -> str:
    """
    Extracts and concatenates all Markdown cells from a Jupyter Notebook (.ipynb) file.
    """
    try:
        nb = nbformat.read(filepath, as_version=4)
    except Exception as e:
        print(f"🚫 Failed to read the notebook file: {e}")
        sys.exit(1)

    markdown_cells = [cell['source'] for cell in nb.cells if cell['cell_type'] == 'markdown']
    markdown_content = '\n\n'.join(markdown_cells)
    return markdown_content

def validate_notebook(filepath: str):
    """Validates a single notebook file."""
    if not os.path.isfile(filepath) or not filepath.endswith('.ipynb'):
        print(f"🚫 Skipping {filepath}: Not a valid .ipynb file.")
        return
    
    content = extract_markdown_from_ipynb(filepath)
    if content:
        validator = NotebookValidator(content,filepath)
        validator.validate()

def validate_folder(folder_path: str):
    """
    Validates all notebooks in a folder and generates a summary.
    
    For each .ipynb in the folder:
    1) Calls validate_notebook(file_path), which is assumed to create an _error.txt file if needed.
    2) Tracks how many files are valid or invalid.
    3) Logs error details, including an error count for each file with issues, into a summary.
    """
    # Counters
    total_files = 0
    error_files = 0
    correct_files = 0

    # This list will hold the per-file error summaries
    summary_errors = []

    print(f"📂 Validating all notebooks in folder: {folder_path}\n")

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        # Only process .ipynb files
        if file_path.endswith('.ipynb'):
            total_files += 1

            # 1) Call your function that validates a single notebook
            validate_notebook(file_path)

            # 2) Check if an error file was produced
            error_file = f"{os.path.splitext(file_path)[0]}_errors.txt"
            if os.path.exists(error_file):
                error_files += 1

                # Read the errors and count them
                with open(error_file, 'r', encoding='utf-8') as f:
                    lines = f.read().splitlines()
                
                # Count only non-empty lines as errors, or adjust logic as needed
                # depending on how your _error.txt is structured.
                error_count = sum(bool(line.strip()) for line in lines)
                
                # Append a summary line for this file, including how many errors it has
                summary_errors.append(
                    f"{file_name} - Found {error_count} error(s):\n" +
                    "\n".join(lines) + "\n"
                )
            else:
                correct_files += 1

    # 3) Generate a combined summary file
    summary_file = os.path.join(folder_path, "summary_errors.txt")

    # If we have any errors, write them out
    if summary_errors:
        with open(summary_file, 'w', encoding='utf-8') as f:
            # Write a short header summarizing overall stats
            f.write(f"Total files: {total_files}\n")
            f.write(f"Correct files: {correct_files}\n")
            f.write(f"Error files: {error_files}\n")
            f.write("\nDetailed Errors:\n")
            f.write("".join(summary_errors))

        print("\n📜 Summary of errors saved in", summary_file)
        print(f"  - Total files: {total_files}")
        print(f"  - Correct files: {correct_files}")
        print(f"  - Error files: {error_files}")
    else:
        # If no errors, remove any old summary file and just print final stats
        if os.path.exists(summary_file):
            os.remove(summary_file)
        print("No errors found! \n")
        print(f"  - Total files: {total_files}")
        print(f"  - Correct files: {correct_files}")
        print(f"  - Error files: {error_files}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python validate_notebook.py <path_to_notebook>")
        sys.exit(1)
    input_path = sys.argv[1]
    if os.path.isdir(input_path):
        validate_folder(input_path)
    elif os.path.isfile(input_path):
        validate_notebook(input_path)
    else:
        print("🚫 Unsupported file format. Please provide a .ipynb file or folder.")
        sys.exit(1)

if __name__ == "__main__":
    main()
