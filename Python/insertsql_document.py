import xml.etree.ElementTree as ET

def partial_flatten(element, prefix=""):
    """Flattens specific elements into a dictionary."""
    data = {}
    for child in element:
        key = prefix + child.tag.split('}')[-1]  # Remove namespace
        if child.text and child.text.strip():
            data[key] = child.text.strip()
        else:
            data.update(partial_flatten(child, key + '/'))
        # Include attributes in the flattened data
        for attr_key, attr_value in child.attrib.items():
            data[f"{key}/@{attr_key}"] = attr_value
    return data

# Path to the XML file
xml_file_path = 'G:\\samanth473_drive\\CDP\\CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1\\examples\\samples\\POC_xml.xml'

# Parse the XML file
try:
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
except FileNotFoundError:
    print(f"Error: File '{xml_file_path}' not found.")
    exit()
except ET.ParseError as e:
    print(f"Error: Failed to parse XML file. {str(e)}")
    exit()

# Namespace dictionary for the XML file
namespace = {'cda': 'urn:hl7-org:v3'}

# Function to find and flatten elements
def find_and_flatten(path):
    element = root.find(path, namespace)
    return partial_flatten(element, '') if element is not None else {}

# Extract and flatten data
document_data = partial_flatten(root, 'document/')
patient_data = find_and_flatten('.//cda:patientRole')
guardian_data = find_and_flatten('.//cda:guardian')
author_data = find_and_flatten('.//cda:author')
custodian_data = find_and_flatten('.//cda:custodian')
encounter_data = find_and_flatten('.//cda:componentOf')
component_data = find_and_flatten('.//cda:component')

# Print the data in a readable format
def print_data(title, data):
    print(f"\n{title}:")
    if data:
        for key, value in data.items():
            print(f"{key}: {value}")
    else:
        print("No data found.")

print_data("Document Information", document_data)
print_data("Patient Information", patient_data)
print_data("Guardian Information", guardian_data)
print_data("Author Information", author_data)
print_data("Custodian Information", custodian_data)
print_data("Encounter Information", encounter_data)
print_data("Component Information", component_data)
