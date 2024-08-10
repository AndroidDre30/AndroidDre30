import os
import re
import ast

def extract_and_save_sensitive_info(input_folder, output_folder):
    """
    Extract sensitive information from .py files in the input folder,
    specifically handling configurations in dictionary format and function calls,
    and save the extracted data to separate files in the output folder.
    """
    SENSITIVE_CATEGORIES = {
        'database': [r'database|db|host|port|user|username|password'],
        'password': [r'password|passwd|pwd|secret'],
        'sensitive_key': [r'key|api|token|auth|jwt'],
        'client_credentials': [r'client|secret|id|credential'],
        'encrypted': [r'\b(encrypted|ciphertext|cipher|crypto|secure|enc)\b']
    }

    DICT_PATTERN = re.compile(r'config\s*=\s*\{(.*?)\}', re.DOTALL)
    FUNCTION_CALL_PATTERN = re.compile(r'\b(?:decrypt_and_update_ssn|encrypt_and_store)\s*\((.*?)\)', re.DOTALL)
    ENCRYPTED_PATTERN = re.compile(r'\b(encrypted|ciphertext|cipher|crypto|secure|enc)\b', re.IGNORECASE)

    def parse_dict(content):
        try:
            return ast.literal_eval('{' + content + '}')
        except (SyntaxError, ValueError):
            return {}

    def extract_from_dict(content, info):
        for key, value in parse_dict(content).items():
            key_str, value_str = str(key), str(value)
            for category, patterns in SENSITIVE_CATEGORIES.items():
                if any(re.search(pat, key_str, re.IGNORECASE) for pat in patterns):
                    info[category].append(f"{key_str}={value_str}")
            if ENCRYPTED_PATTERN.search(value_str):
                info['encrypted'].append(f"{key_str}={value_str}")

    def extract_from_line(line, info):
        if '=' in line and not line.startswith('#'):
            key, value = map(str.strip, line.split('=', 1))
            for category, patterns in SENSITIVE_CATEGORIES.items():
                if any(re.search(pat, key, re.IGNORECASE) for pat in patterns):
                    info[category].append(f"{key}={value}")
            if ENCRYPTED_PATTERN.search(value):
                info['encrypted'].append(f"{key}={value}")

    def extract_from_function_call(content, info):
        for match in FUNCTION_CALL_PATTERN.finditer(content):
            full_call = match.group(0)
            args = match.group(1).split(',')  # Split by comma
            info['encrypted'].append(full_call)
            for arg in args:
                arg = arg.strip().replace(',', '')  # Remove commas
                if ENCRYPTED_PATTERN.search(arg):
                    info['encrypted'].append(arg)
                else:
                    for category, patterns in SENSITIVE_CATEGORIES.items():
                        if any(re.search(pat, arg, re.IGNORECASE) for pat in patterns):
                            info[category].append(arg)

    for root, _, files in os.walk(input_folder):
        if root == input_folder:
            for filename in files:
                if filename.endswith('.py'):
                    file_path = os.path.join(root, filename)
                    extracted_info = {category: [] for category in SENSITIVE_CATEGORIES}
                    extracted_info['encrypted'] = []

                    try:
                        with open(file_path, 'r') as file:
                            file_content = file.read()
                            for match in DICT_PATTERN.finditer(file_content):
                                extract_from_dict(match.group(1), extracted_info)
                            for line in file_content.splitlines():
                                extract_from_line(line.strip(), extracted_info)
                            extract_from_function_call(file_content, extracted_info)

                        output_file_path = os.path.join(output_folder, os.path.splitext(os.path.relpath(file_path, input_folder))[0] + '.py')
                        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
                        with open(output_file_path, 'w') as file:
                            for category, items in extracted_info.items():
                                file.write(f"# {category.upper()}\n")
                                file.writelines(f"{item}\n" for item in items)
                                file.write("\n")

                        print(f"Sensitive information saved to {output_file_path}")

                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

# Example usage:
if __name__ == '__main__':
    input_folder = r'C:\Users\kiandret\PycharmProjects\Tes Folder'
    output_folder = r'C:\Users\kiandret\PycharmProjects\pythonProject\Secret Extractor Contents'
    extract_and_save_sensitive_info(input_folder, output_folder)
