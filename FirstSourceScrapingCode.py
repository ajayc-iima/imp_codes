import os
import glob
import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException


def setup_driver(download_directory):
    """Initializes the WebDriver with specific download settings for Edge."""
    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--start-maximized")
    edge_options.add_experimental_option("prefs", {
        "download.default_directory": download_directory,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    return webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()), options=edge_options)


def safe_print(*args):
    """Thread-safe print function."""
    print(*args)


def create_directory(path):
    """Creates a directory if it doesn't already exist."""
    if not os.path.exists(path):
        os.makedirs(path)


def format_filename(text):
    """Converts text to a filename-friendly format."""
    formatted = text.replace(' ', '_').replace('&', 'and')
    formatted = ''.join(c for c in formatted if c.isalnum() or c == '_')
    return formatted


def rename_downloaded_file(download_directory, state_name, business_type=None, page_number=None):
    """Renames the downloaded file with appropriate naming convention."""
    downloaded_file_path = os.path.join(download_directory, 'data.csv')
    if os.path.exists(downloaded_file_path):
        state_formatted = format_filename(state_name)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        if business_type:
            business_formatted = format_filename(business_type)
            new_filename = f"{state_formatted}_{business_formatted}_page_{page_number}.csv"
        else:
            new_filename = f"{state_formatted}_{timestamp}.csv"
            
        new_file_path = os.path.join(download_directory, new_filename)
        os.rename(downloaded_file_path, new_file_path)
        safe_print(f"Renamed downloaded file to: {new_filename}")
        return new_file_path
    else:
        safe_print("No file found to rename.")
        return None


def move_files_to_directory(download_directory, state_name, business_type=None):
    """Moves CSV files to appropriate directory structure."""
    state_formatted = format_filename(state_name)
    
    if business_type:
        business_formatted = format_filename(business_type)
        target_directory = os.path.join(download_directory, f"{state_formatted}_{business_formatted}")
        file_pattern = f"{state_formatted}_{business_formatted}_page_*.csv"
    else:
        target_directory = os.path.join(download_directory, state_formatted)
        file_pattern = f"{state_formatted}_*.csv"
    
    create_directory(target_directory)
    csv_files = glob.glob(os.path.join(download_directory, file_pattern))
    
    for file_path in csv_files:
        try:
            destination_path = os.path.join(target_directory, os.path.basename(file_path))
            os.rename(file_path, destination_path)
            safe_print(f"Moved file to {target_directory}: {os.path.basename(file_path)}")
        except Exception as e:
            safe_print(f"Error moving file {file_path}: {e}")


def combine_csv_files(download_directory, state_name, business_type=None):
    """Combines CSV files into a single file."""
    state_formatted = format_filename(state_name)
    
    if business_type:
        business_formatted = format_filename(business_type)
        directory_path = os.path.join(download_directory, f"{state_formatted}_{business_formatted}")
        file_pattern = f"{state_formatted}_{business_formatted}_page_*.csv"
        output_filename = f"{state_formatted}_{business_formatted}_combined.csv"
    else:
        directory_path = os.path.join(download_directory, state_formatted)
        file_pattern = f"{state_formatted}_*.csv"
        output_filename = f"{state_formatted}_combined.csv"

    csv_files = glob.glob(os.path.join(directory_path, file_pattern))
    if business_type:
        csv_files.sort(key=lambda x: int(x.split('page_')[-1].split('.')[0]))
    else:
        csv_files.sort(key=os.path.getmtime, reverse=True)

    combined_df = pd.DataFrame()
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except Exception as e:
            safe_print(f"Error processing {file_path}: {e}")

    if not combined_df.empty:
        combined_file_path = os.path.join(directory_path, output_filename)
        combined_df.to_csv(combined_file_path, index=False, encoding='utf-8')
        safe_print(f"Created combined CSV file: {combined_file_path}")
    else:
        safe_print(f"No data to combine for {output_filename}")


def consolidate_state_files(download_directory, state_name):
    """Consolidates all combined CSV files for a state across business types."""
    state_formatted = format_filename(state_name)
    combined_df = pd.DataFrame()

    # Search for all business type folders for the state
    state_directory_pattern = os.path.join(download_directory, f"{state_formatted}_*")
    business_type_folders = glob.glob(state_directory_pattern)

    if not business_type_folders:
        safe_print(f"No business type folders found for state: {state_formatted}")
        return

    safe_print(f"Found {len(business_type_folders)} folders for state: {state_formatted}")

    for folder in business_type_folders:
        safe_print(f"Processing folder: {folder}")
        combined_files = glob.glob(os.path.join(folder, "*_combined.csv"))
        
        for file_path in combined_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                safe_print(f"Processed: {file_path}")
            except Exception as e:
                safe_print(f"Error processing {file_path}: {e}")

    if not combined_df.empty:
        consolidated_file_path = os.path.join(download_directory, f"{state_formatted}_consolidated.csv")
        combined_df.to_csv(consolidated_file_path, index=False, encoding='utf-8')
        safe_print(f"Created consolidated CSV file: {consolidated_file_path}")
    else:
        safe_print(f"No data to consolidate for state: {state_formatted}")


def download_and_process_data(driver, download_directory, state_name, business_type=None):
    """Handles the download and processing of data for a state/business combination."""
    try:
        if business_type:
            category_tab = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@class='nav-link' and @href='#category']"))
            )
            category_tab.click()
            time.sleep(2)

            business_select = Select(driver.find_element(By.ID, "nature_of_business"))
            business_select.select_by_visible_text(business_type)
            time.sleep(2)

        location_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[@class='nav-link' and @href='#location']"))
        )
        location_tab.click()
        time.sleep(2)

        state_select = Select(driver.find_element(By.ID, "state_name"))
        state_select.select_by_visible_text(state_name)
        time.sleep(2)

        submit_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "frm_submit"))
        )
        submit_button.click()

        page_number = 1
        while True:
            try:
                time.sleep(5)
                export_csv_link = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(@id, 'exporttocsv_') and contains(text(), 'Export to CSV')]"))
                )
                export_csv_link.click()

                WebDriverWait(driver, 40).until(
                    lambda d: os.path.exists(os.path.join(download_directory, 'data.csv'))
                )
                rename_downloaded_file(download_directory, state_name, business_type, page_number)

                next_button = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@class='img_but_next']"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)

                try:
                    overlay = WebDriverWait(driver, 3).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, "a.nav-link.dropdown-toggle"))
                    )
                    overlay.click()
                    time.sleep(1)
                except (TimeoutException, NoSuchElementException):
                    pass

                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(5)
                page_number += 1
                WebDriverWait(driver, 20).until(EC.staleness_of(next_button))
            except Exception as e:
                safe_print(f"Navigation ended. Moving to next combination. Error: {e}")
                break

        move_files_to_directory(download_directory, state_name, business_type)
        combine_csv_files(download_directory, state_name, business_type)

    except Exception as e:
        safe_print(f"Error in download_and_process_data: {e}")
        raise


def main():
    download_directory = r"D:\Downloads"

    # States that need business type filtering
    business_filtered_states = [
        "Maharashtra", "NCT of Delhi", "Rajasthan", "Tamil Nadu", "Telangana",
        "Uttar Pradesh", "Gujarat", "Haryana", "Karnataka", "West Bengal",
    ]

    # Other states that don't need business type filtering
    other_states = [
        "Andaman & Nicobar", "Manipur", "Meghalaya", "Mizoram", "Nagaland", "Odisha",
        "Puducherry", "Punjab", "Sikkim", "Tripura", "Uttarakhand", "Madhya Pradesh",
        "Lakshadweep", "Ladakh", "Andhra Pradesh", "Arunachal Pradesh", "Assam",
        "Bihar", "Chandigarh", "Chhattisgarh", "Dadra & Nagar Haveli", "Daman & Diu",
        "Goa", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Kerala"
    ]

    business_types = [
        "Agriculture and Allied Activities",
        "Business Services",
        "Community, personal & Social Services",
        "Construction",
        "Electricity, Gas & Water companies",
        "Finance",
        "Insurance",
        "Manufacturing (Food stuffs)",
        "Manufacturing (Leather & products thereof)",
        "Manufacturing (Machinery & Equipments)",
        "Manufacturing (Metals & Chemicals, and products thereof)",
        "Manufacturing (Others)",
        "Manufacturing (Paper & Paper products, Publishing, printing and reproduction of recorded media)",
        "Manufacturing (Textiles)",
        "Manufacturing (Wood Products)",
        "Mining & Quarrying",
        "Real Estate and Renting",
        "Trading",
        "Transport, storage and Communications"
    ]

    driver = setup_driver(download_directory)
    driver.get("https://firstsource.cmie.com/kommon/bin/sr.php?kall=wadvsearch")
    input("Press Enter after you've logged in...")

    try:
        # Process states that need business type filtering
        for state in business_filtered_states:
            safe_print(f"\nProcessing {state} with business type filtering...")
            for business_type in business_types:
                # Skip "Business Services" for Maharashtra
                if state == "Maharashtra" and business_type == "Business Services":
                    safe_print("Skipping Business Services for Maharashtra...")
                    continue
                    
                try:
                    driver.get("https://firstsource.cmie.com/kommon/bin/sr.php?kall=wadvsearch")
                    time.sleep(2)
                    download_and_process_data(driver, download_directory, state, business_type)
                    safe_print(f"Completed processing {state} - {business_type}")
                except Exception as e:
                    safe_print(f"Error processing {state} - {business_type}: {e}")
                    continue
            
            # Consolidate files for this state after all business types are processed
            safe_print(f"\nConsolidating files for {state}...")
            consolidate_state_files(download_directory, state)

        # Process other states without business type filtering
        for state in other_states:
            safe_print(f"\nProcessing {state} without business type filtering...")
            try:
                driver.get("https://firstsource.cmie.com/kommon/bin/sr.php?kall=wadvsearch")
                time.sleep(2)
                download_and_process_data(driver, download_directory, state)
                safe_print(f"Completed processing {state}")
            except Exception as e:
                safe_print(f"Error processing {state}: {e}")
                continue

        safe_print("\nData collection and consolidation completed for all states.")
    except Exception as e:
        safe_print(f"An error occurred in main execution: {e}")
    finally:
        input("Press Enter to close the browser...")
        driver.quit()


if __name__ == "__main__":
    main()