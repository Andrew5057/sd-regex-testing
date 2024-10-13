"""An interactive CLI tool that wraps the sd-regex-testing library.
"""

import argparse
from typing import Literal

import sd_regex_testing as sdrt

def test_from_cli():
    """Launch an interactive regex testing session.
    """
    parser = argparse.ArgumentParser(prog='sdrt', description="Interactively "
                                     + "test regexes against metasmoke data")
    parser.add_argument('file', type=str, help="the path to a metasmoke JSON file")
    args = parser.parse_args()
    
    session = InteractiveSession(args.file)
    
    session.start()

class InteractiveSession:
    """An interactive SDRT session where users can test various regexes against
    a given body of posts.
    """
    
    def __init__(self, file: str):
        self.file = file
        self.posts = sdrt.read_json(file)
        self.last_regex = ""
        self.last_test_type = "placeholder"
    
    def start(self):
        """Start the interactive session. First provides the user with a
        the list of possible commands, then indefinitely allows the user to
        enter them.
        
        Commands can either take the form "test type regex" (test a regex)
        or "tp|fp|tn|fn|summarize" (get information about the last test).
        """
        
        print(f"""
Started an interactive SDRT session with {self.file} ({len(self.posts)} posts)
Commands:
- "test (title|username|keyword|website) regex" to store a regex test
- "tp|fp|tn|fn" to get the number of each type of result
- "summarize" to see a quick summary of the results
- "exit" or CTRL+C to exit""")
        try:
            while True:
                self._parse_command(input(">>> "))
        except KeyboardInterrupt:
            return
    
    def test_regex(self, test_type: Literal['title', 'username', 'keyword', 'website'],
                   regex: str) -> None:
        """Store the result of a regex test.

        :param test_type: the type of test.
        :type test_type: Literal['title', 'username', 'keyword', 'website']
        :param regex: the regex to test.
        :type regex: str
        """

        match test_type:
            case 'title':
                self.posts = self.posts.sdrt.test_title(regex)
            case 'username':
                self.posts = self.posts.sdrt.test_username(regex)
            case 'keyword':
                self.posts = self.posts.sdrt.test_keyword(regex)
            case 'website':
                self.posts = self.posts.sdrt.test_website(regex)
            case _:
                return
        self.last_regex = regex
        self.last_test_type = test_type
        print(f"Tested '{regex}' as a {test_type}")
    
    def _parse_command(self, command: str) -> None:
        """Convert a command from an interactive session into its corresponding
        DataFrame output. Accepted inputs are:
        - "tp", "fp", "tn", "fn" (prints the filtered posts)
        - "test (title|username|keyword|website) regex" (stores the regex
          test in the posts field)
        
        :param command: the inputted line of text
        :type command: str
        """
        command_raw = command.split(maxsplit=2)
        if command_raw[0] in {'tp', 'fp', 'tn', 'fn'}:
            print(len(getattr(self.posts.sdrt, command_raw[0])))
        match command_raw[0]:
            case "summarize":
                print(f"Regex '{self.last_regex}' as a "
                      + f"{self.last_test_type} yielded "
                      + f"{len(self.posts.sdrt.tp)} TP, "
                      + f"{len(self.posts.sdrt.fp)} FP, "
                      + f"{len(self.posts.sdrt.tn)} TN, and "
                      + f"{len(self.posts.sdrt.fn)} FN")
            case "test":
                if len(command_raw) < 3:
                    return
                self.test_regex(command_raw[1], command_raw[2])
            case "exit":
                raise KeyboardInterrupt() # Gets caught by exception handling
