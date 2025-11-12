#!/usr/bin/env python3
"""
DroneScape Data Transfer & Backup Protocol
TERN Australia - UAV TEAM Data Management System

This script facilitates the automated transfer and verification of UAV data
collected for the DroneScape program at TERN Australia plots.

Author: TERN DroneScape Team
Version: 2.0.0
Last Modified: 2025
Python Version: 3.7+

Usage:
    python dronescape_transfer.py

Features:
    - Automated folder structure creation
    - LiDAR (L2) and RGB imagery (P1) data transfer, MICASENSE TO BE ADDED AT LATER DATE
    - Comprehensive data integrity verification
    - Automated SSD backup with comparison
    - Progress tracking and transfer statistics
"""

print("DEBUG: Starting script...")

import os
import sys
import json
import shutil
import time
import logging
from datetime import datetime, date
from typing import Dict, List, Set, Tuple, Optional, Any
from pathlib import Path

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("WARNING: PIL/Pillow not available. MicaSense metadata reading will be limited.")

print("DEBUG: All imports successful")

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

CONFIG_FILE = "dronescape_config.json"

# LOG_FILE will be set after user input
LOG_FILE = None

print(f"DEBUG: Config file: {CONFIG_FILE}")

# Required file extensions for validation
LIDAR_REQUIRED_FILES = ["MRK", "RPT", "CLC", "CLI", "DBG", "IMU", "LDR", 
                        "LDRT", "RPOS", "RTB", "RTK", "RTL", "RTS", "SIG", "JPG"]
P1_REQUIRED_FILES = ["NAV", "OBS", "BIN", "MRK", "JPG"]
MICASENSE_REQUIRED_FILES = ["TIF", "TIFF"]

# MicaSense camera serial numbers
MICASENSE_RED_SERIAL = "PR03-2117857-MS"
MICASENSE_BLUE_SERIAL = "PB01-2310044-MS"

# Development mode flag (bypasses actual file operations)
DEV_MODE = False

print("DEBUG: Constants defined")

# ============================================================================
# ANSI COLOR CODES FOR TERMINAL OUTPUT
# ============================================================================

class Colors:
    """ANSI color codes for terminal formatting."""
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    WHITE = '\033[97m'
    PURPLE = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

# ============================================================================
# VALID TERN PLOT DATABASE
# ============================================================================

SITE_DATABASE: Set[str] = {
'SAAFLB0030', 'SAAFLB0031' , 'SAAMDD0007' , 'SAAMDD0008' , 'SAAMDD0009' , 'SAAMDD0010' , 'SAAMDD0011' , 'NTAFIN0002' , 'NTAFIN0003' , 'NTAFIN0005' , 'NTAFIN0004' , 'NTAFIN0006' , 'NTAFIN0007' , 'NTAFIN0008' , 'NTAFIN0009' , 'NTAFIN0010' , 'NTAFIN0001' , 'NTAFIN0011' , 'NTAFIN0012' , 'NTAFIN0013' , 'NTAFIN0014' , 'NTAFIN0015' , 'NTAFIN0016' , 'NTAFIN0017' , 'NTAFIN0018' , 'NTAFIN0019' , 'NTAFIN0020' , 'NTAFIN0021' , 'NTAFIN0022' , 'NTAFIN0023' , 'TCFTSR0001' , 'TCFTNS0001' , 'TCFTNS0002' , 'TCFTSR0002' , 'NTAFIN0024' , 'NTAFIN0025' , 'SASMDD0001' , 'SASMDD0003' , 'SASMDD0002' , 'SASMDD0005' , 'SASMDD0006' , 'SASMDD0011' , 'SASMDD0004' , 'SASMDD0013' , 'SASMDD0001' , 'SASMDD0002' , 'SASMDD0009' , 'SASMDD0010' , 'SASMDD0008' , 'NTAFIN0026', 'NTAFIN0027' , 'NTABRT0001' , 'NTABRT0002' , 'NTABRT0003' , 'NTABRT0004' , 'NTABRT0005' , 'NTABRT0006' , 'NTAGFU0001' , 'NTAGFU0002' , 'NTAGFU0003' , 'NTAGFU0004' , 'NTAGFU0005' , 'NTAGFU0006' , 'NTAGFU0007' , 'NTAGFU0008' , 'NTAGFU0009' , 'NTAGFU0010' , 'NTAGFU0011' , 'NTAGFU0012' , 'NTAGFU0013' , 'NTAGFU0014' , 'NTAGFU0015' , 'NTAGFU0016' , 'NTAGFU0017' , 'NTAGFU0018' , 'NTAGFU0019' , 'NSABHC0001' , 'NSABHC0002' , 'NSABHC0003' , 'NSABHC0004' , 'NSABHC0005' , 'NSABHC0006' , 'NSABHC0007' , 'NSABHC0008' , 'NSABHC0009' , 'NSABHC0010' , 'NSABHC0011',
}

print(f"DEBUG: Site database loaded with {len(SITE_DATABASE)} sites")

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

print("DEBUG: Setting up logging...")

def setup_logging() -> None:
    """Configure logging to file and console."""
    global LOG_FILE
    try:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logging.info("DroneScape Transfer Protocol initiated")
        logging.info(f"Log file: {LOG_FILE}")
    except Exception as e:
        print(f"Warning: Could not initialize logging: {e}")
        # Continue without logging rather than crash

def get_log_directory() -> str:
    """
    Get or set the log file directory.
    
    Returns:
        Path to log directory
    """
    global LOG_FILE
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}SET LOG FILE DIRECTORY{Colors.RESET}")
    print(f"{'─' * 90}")
    
    if DEV_MODE:
        mock_path = "C:\\TERN\\MockLogs"
        log_filename = f"dronescape_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        LOG_FILE = os.path.join(mock_path, log_filename)
        print(f"{Colors.CYAN}[DEV MODE] Using mock log directory: {LOG_FILE}{Colors.RESET}")
        return mock_path
    
    saved_log_dir = load_config("log_directory")
    
    if saved_log_dir and os.path.isdir(saved_log_dir):
        print(f"\n{Colors.CYAN}CURRENT LOG DIRECTORY: {saved_log_dir}{Colors.RESET}")
        choice = input(f"PRESS {Colors.YELLOW}ENTER{Colors.RESET} TO KEEP, OR PASTE A NEW DIRECTORY:").strip()
        
        if choice:
            while not os.path.isdir(choice):
                print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
                choice = input("Paste directory: ").strip()
            log_dir = choice
        else:
            log_dir = saved_log_dir
    else:
        log_dir = input("Paste log file directory: ").strip()
        while not os.path.isdir(log_dir):
            print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
            log_dir = input("Paste directory: ").strip()
    
    save_config("log_directory", log_dir)
    
    # Create log filename with timestamp
    log_filename = f"dronescape_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    LOG_FILE = os.path.join(log_dir, log_filename)
    
    print(f"{Colors.GREEN}Log directory set to: {log_dir}{Colors.RESET}")
    print(f"{Colors.GREEN}Log file will be: {LOG_FILE}{Colors.RESET}")
    logging.info(f"Log directory set: {log_dir}")
    return log_dir

print("DEBUG: Logging setup function defined")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

print("DEBUG: Defining utility functions...")

def clear_screen() -> None:
    """Clear terminal screen (cross-platform)."""
    os.system('cls' if os.name == 'nt' else 'clear')

def format_size(bytes_size: float) -> str:
    """
    Format bytes into human-readable size.
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Formatted string (e.g., "1.23 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def format_speed(bytes_per_sec: float) -> str:
    """
    Format transfer speed into human-readable format.
    
    Args:
        bytes_per_sec: Speed in bytes per second
        
    Returns:
        Formatted string (e.g., "45.2 MB/s")
    """
    if bytes_per_sec < 1024:
        return f"{bytes_per_sec:.2f} B/s"
    elif bytes_per_sec < 1024**2:
        return f"{bytes_per_sec/1024:.2f} KB/s"
    elif bytes_per_sec < 1024**3:
        return f"{bytes_per_sec/(1024**2):.2f} MB/s"
    else:
        return f"{bytes_per_sec/(1024**3):.2f} GB/s"

def format_time(seconds: float) -> str:
    """
    Format time duration into human-readable format.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted string (e.g., "5m 32s")
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    
    if mins < 60:
        return f"{mins}m {secs}s"
    
    hours = mins // 60
    mins = mins % 60
    return f"{hours}h {mins}m {secs}s"

def get_folder_size(folder_path: str) -> int:
    """
    Calculate total size of a folder and its contents.
    
    Args:
        folder_path: Path to the folder
        
    Returns:
        Total size in bytes
    """
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
    except (OSError, PermissionError) as e:
        logging.error(f"Error calculating folder size for {folder_path}: {e}")
    return total_size

def check_file_corruption(file_path: str) -> bool:
    """
    Check if a file is potentially corrupted.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if file appears corrupted, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            return True
        if os.path.getsize(file_path) == 0:
            return True
        with open(file_path, "rb") as f:
            f.read(1024)  # Try to read first KB
        return False
    except (OSError, IOError, PermissionError) as e:
        logging.warning(f"File corruption check failed for {file_path}: {e}")
        return True

def color_status(status: str) -> str:
    """
    Apply color coding to status messages.
    
    Args:
        status: Status string
        
    Returns:
        Colored status string
    """
    if status == "Copied":
        return f"{Colors.GREEN}{status}{Colors.RESET}"
    elif status == "Already exists":
        return f"{Colors.YELLOW}{status}{Colors.RESET}"
    elif status.startswith(("Error", "Missing")):
        return f"{Colors.RED}{status}{Colors.RESET}"
    return status

print("DEBUG: All utility functions defined")

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

print("DEBUG: Defining configuration management...")

def load_config(key: str) -> Optional[Any]:
    """
    Load a configuration value from the config file.
    
    Args:
        key: Configuration key to retrieve
        
    Returns:
        Configuration value or None if not found
    """
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding='utf-8') as f:
                config = json.load(f)
                return config.get(key)
        except (json.JSONDecodeError, IOError) as e:
            logging.error(f"Error loading config: {e}")
    return None

def save_config(key: str, value: Any) -> None:
    """
    Save a configuration value to the config file.
    
    Args:
        key: Configuration key
        value: Value to save
    """
    data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    data[key] = value
    data["last_updated"] = datetime.now().isoformat()
    
    try:
        with open(CONFIG_FILE, "w", encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Configuration saved: {key}")
    except IOError as e:
        logging.error(f"Error saving config: {e}")

print("DEBUG: Configuration management defined")

# ============================================================================
# DISPLAY FUNCTIONS
# ============================================================================

print("DEBUG: Defining display functions...")

def show_title() -> None:
    """Display the application title banner."""
    clear_screen()
    terminal_width = 120
    
    title = f"""{Colors.CYAN}{Colors.BOLD}
       ███                              ██████████                              ███  
      ╔███                             ██╔══════╗██                             ███╗ 
      █  █╗                           ██          ██                           ╔█  █
    ╔██   ██████═════════════════════╝██          ██╚═════════════════════██████   ██╗
    ███    █████████████████████████████          █████████████████████████████    ███
   ╔██      ██╔═══════════════════╗████            ████╔════════════════════╗██     ██╗
   ╚═       ══╝                     ███            ███                      ╚══      ═╝
                                    ██████████████████
                                   ███     ████     ███                            
                                  ███       ██       ███                             
                                 ███    ╔════════╗    ███                          
                                ███    ╔║  ╔══╗  ║╗    ███                          
                               ███     ╚║  ╚══╝  ║╝     ███                       
                            ╔███╗       ╚════════╝       ╔███╗                      
                           ╔█████╗                      ╔█████╗                   
                           ╚═════╝                      ╚═════╝  
                           
           ████████╗███████╗██████╗ ███╗   ██╗    ██╗   ██╗ █████╗ ██╗   ██╗
           ╚══██╔══╝██╔════╝██╔══██╗████╗  ██║    ██║   ██║██╔══██╗██║   ██║
              ██║   █████╗  ██████╔╝██╔██╗ ██║    ██║   ██║███████║██║   ██║
              ██║   ██╔══╝  ██╔══██╗██║╚██╗██║    ██║   ██║██╔══██║╚██╗ ██╔╝
              ██║   ███████╗██║  ██║██║ ╚████║    ╚██████╔╝██║  ██║ ╚████╔╝ 
              ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝     ╚═════╝ ╚═╝  ╚═╝  ╚═══╝  

    ██████╗ ██████╗  ██████╗ ███╗   ██╗███████╗███████╗ ██████╗ █████╗ ██████╗ ███████╗
    ██╔══██╗██╔══██╗██╔═══██╗████╗  ██║██╔════╝██╔════╝██╔════╝██╔══██╗██╔══██╗██╔════╝
    ██║  ██║██████╔╝██║   ██║██╔██╗ ██║█████╗  ███████╗██║     ███████║██████╔╝█████╗  
    ██║  ██║██╔══██╗██║   ██║██║╚██╗██║██╔══╝  ╚════██║██║     ██╔══██║██╔═══╝ ██╔══╝  
    ██████╔╝██║  ██║╚██████╔╝██║ ╚████║███████╗███████║╚██████╗██║  ██║██║     ███████╗
    ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚══════╝
{Colors.YELLOW}
   █▀█ █ █ ▀█▀ █▀█ █▄▄█ █▀█ ▀█▀ █▀▀ █▀▄   █▀▄ █▀█ ▀█▀ █▀█   ▀█▀ █▀▄ █▀█ █▀█ █▀▀ █▀▀ █▀▀ █▀▄
   █▀█ █ █  █  █ █ █  █ █▀█  █  █▀▀ █ █   █ █ █▀█  █  █▀█    █  █▀▄ █▀█ █ █ ▀▀█ █▀▀ █▀▀ █▀▄
   ▀ ▀ ▀▀▀  ▀  ▀▀▀ ▀  ▀ ▀ ▀  ▀  ▀▀▀ ▀▀    ▀▀  ▀ ▀  ▀  ▀ ▀    ▀  ▀ ▀ ▀ ▀ ▀ ▀ ▀▀▀ ▀   ▀▀▀ ▀ ▀
           █▀█ █▀█ █▀▄   █▀▄ █▀█ █▀▀ █ █ █ █ █▀█   █▀█ █▀▄ █▀█ ▀█▀ █▀█ █▀▀ █▀█ █           
           █▀█ █ █ █ █   █▀▄ █▀█ █   █▀▄ █ █ █▀▀   █▀▀ █▀▄ █ █  █  █ █ █   █ █ █
           ▀ ▀ ▀ ▀ ▀▀    ▀▀  ▀ ▀ ▀▀▀ ▀ ▀ ▀▀▀ ▀     ▀   ▀ ▀ ▀▀▀  ▀  ▀▀▀ ▀▀▀ ▀▀▀ ▀▀▀                 
{Colors.RESET}"""
    version = f"{Colors.YELLOW}Version 2.0{Colors.RESET}"
    
    print(title)
    print(f"    {version.center(90)}\n")
    print(f"{Colors.GREEN}{'═' * 90}{Colors.RESET}")
    
    # 70s arcade-style countdown sequence
    countdown_messages = [
        (f"{Colors.CYAN}Initiating....5{Colors.RESET}", 1.0),
        (f"{Colors.CYAN}Booting up...4{Colors.RESET}", 1.0),
        (f"{Colors.YELLOW}Aligning crystals...3{Colors.RESET}", 1.0),
        (f"{Colors.YELLOW}Engaging thrusters...2{Colors.RESET}", 1.0),
        (f"{Colors.PURPLE}Arming lasers...1{Colors.RESET}", 1.0),
        (f"{Colors.GREEN}{Colors.BOLD}Ready Player 1{Colors.RESET}", 1.5)
    ]
    
    print("\n")
    for message, delay in countdown_messages:
        print(f"                              {message}")
        time.sleep(delay)

def show_intro() -> None:
    """Display introductory information."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}BEFORE YOU BEGIN, READ ME:{Colors.RESET}")
    print(f"""
THIS CODE WAS CREATED BY TROY BEKTAS AND IS DESIGNED TO AID THE TRANSFER AND BACKUP OF 
UAV DATA COLLECTED FOR THE DRONESCAPE PROGRAM AT TERN AUS PLOTS AROUND AUSTRALIA. 

DRONESCAPE AIMS TO COLLECT AND PROCESS LiDAR, RGB IMAGERY, AND MULTISPECTRAL IMAGERY.

THIS SCRIPT HAS BEEN DESIGNED TO HANDLE THESE TYPES OF DATA AND TRANSFER THEM ACCORDINGLY 
TO THE DRONESCAPE DATA BACKUP WORKFLOW CREATED BY DOCTOR JUAN CARLOS MONTES HERRERA.
THIS SCRIPT IS ONLY COMPATIBLE WITH DJI AND MICASENSE PRODUCTS SUCH AS THE L2, P1, AND REDEDGE-MX DUEL SENSORS.

{Colors.YELLOW}NOTE:{Colors.RESET} THIS SCRIPT IS DESIGNED FOR INTERNAL USE BETWEEN TERN AUSTRALIA AND UTAS ONLY. 
      FOLDER DIRECTORIES MAY BE SUBJECT TO CHANGE DEPENDING ON WHICH WORKSTATION THE SCRIPT IS BEING RUN ON.{Colors.RESET}""")
    
    print(f"\n{Colors.GREEN}{'═' * 90}{Colors.RESET}")

def print_part_header(part_num: int, title: str) -> None:
    """
    Display enhanced part headers with extra spacing.
    
    Args:
        part_num: Part number
        title: Part title
    """
    print(f"\n\n\n{Colors.GREEN}{Colors.BOLD}")
    print(f"{'╔' + '═' * 88 + '╗'}")
    print(f"║{f'PART {part_num}'.center(88)}║")
    print(f"║{title.center(88)}║")
    print(f"{'╚' + '═' * 88 + '╝'}")
    print(f"{Colors.RESET}\n\n")
    logging.info(f"Started Part {part_num}: {title}")

def print_completion_banner(section_name: str, total_time: Optional[float] = None) -> None:
    """
    Print prominent completion banner.
    
    Args:
        section_name: Name of the completed section
        total_time: Optional total time taken in seconds
    """
    print(f"\n\n{Colors.GREEN}{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
    print(f"║{section_name.center(88)}║")
    if total_time:
        time_str = f"Total time: {format_time(total_time)}"
        print(f"║{time_str.center(88)}║")
    print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")
    logging.info(f"Completed: {section_name}" + (f" in {format_time(total_time)}" if total_time else ""))

def proceed_prompt(message: str = "") -> bool:
    """
    Universal proceed prompt between parts.
    
    Args:
        message: Optional message to display before prompt
        
    Returns:
        True to proceed, False to go back
    """
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Auto-proceeding...{Colors.RESET}")
        time.sleep(0.5)
        return True
    
    if message:
        print(f"\n{Colors.YELLOW}{message}{Colors.RESET}")
    
    while True:
        command = input(f"\n{Colors.YELLOW}TYPE 'PROCEED' TO CONTINUE OR 'BACK' TO GO BACK: {Colors.RESET}").strip().upper()
        if command == "PROCEED":
            return True
        elif command == "BACK":
            return False
        print(f"{Colors.RED}INVALID INPUT. TYPE 'PROCEED' TO CONTINUE OR 'BACK' TO GO BACK.{Colors.RESET}")

print("DEBUG: Display functions defined")

# ============================================================================
# AUTHENTICATION
# ============================================================================

print("DEBUG: Defining authentication...")

def authenticate() -> None:
    """Authenticate user and check for dev mode."""
    global DEV_MODE
    
    command = input(f"\n{Colors.CYAN}TO BEGIN... TYPE {Colors.YELLOW}'SLAY YOUR ENEMIES'{Colors.CYAN}:{Colors.RESET}").strip()
    
    if command == "8":
        DEV_MODE = True
        print(f"{Colors.CYAN}[DEV MODE ENABLED] File operations will be simulated{Colors.RESET}")
        logging.info("Dev mode enabled")
        return
    
    while command != "SLAY YOUR ENEMIES":
        command = input(f"{Colors.RED}INVALID INPUT. PLEASE TYPE 'SLAY YOUR ENEMIES' TO CONTINUE: {Colors.RESET}").strip()
        if command == "8":
            DEV_MODE = True
            print(f"{Colors.CYAN}[DEV MODE ENABLED] File operations will be simulated{Colors.RESET}")
            logging.info("Dev mode enabled")
            return
    
    print(f"{Colors.GREEN}AUTHENTICATION SUCCESSFUL!{Colors.RESET}")
    logging.info("User authenticated successfully")

print("DEBUG: Authentication defined")

# ============================================================================
# DIRECTORY MANAGEMENT
# ============================================================================

print("DEBUG: Defining directory management...")

def get_parent_directory() -> str:
    """
    Get or set the parent directory for data storage.
    
    Returns:
        Path to parent directory
    """
    print(f"\n{Colors.GREEN}{Colors.BOLD}SET PARENT DIRECTORY{Colors.RESET}")
    print(f"{'─' * 90}")
    
    if DEV_MODE:
        mock_path = "C:\\TERN\\MockParent"
        print(f"{Colors.CYAN}[DEV MODE] Using mock parent directory: {mock_path}{Colors.RESET}")
        return mock_path
    
    saved_parent = load_config("parent_folder")
    
    if saved_parent and os.path.isdir(saved_parent):
        print(f"\n{Colors.CYAN}CURRENT PARENT DIRECTORY: {saved_parent}{Colors.RESET}")
        choice = input(f"PRESS {Colors.YELLOW}ENTER{Colors.RESET} TO KEEP, OR PASTE A NEW DIRECTORY:").strip()
        
        if choice:
            while not os.path.isdir(choice):
                print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
                choice = input("Paste directory: ").strip()
            parent = choice
        else:
            parent = saved_parent
    else:
        parent = input("Paste parent directory: ").strip()
        while not os.path.isdir(parent):
            print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
            parent = input("Paste directory: ").strip()
    
    save_config("parent_folder", parent)
    print(f"{Colors.GREEN}PARENT FOLDER SET TO: {parent}{Colors.RESET}")
    logging.info(f"Parent directory set: {parent}")
    return parent

def get_source_directory(data_type: str) -> str:
    """
    Universal source directory getter for L2/P1/Backup.
    
    Args:
        data_type: Type of data source (e.g., "L2 SD CARD", "P1 SD CARD")
        
    Returns:
        Path to source directory
    """
    print(f"\n{Colors.GREEN}{Colors.BOLD}SET {data_type} DIRECTORY{Colors.RESET}")
    print(f"{'─' * 90}")
    
    if DEV_MODE:
        mock_path = f"C:\\TERN\\Mock{data_type.replace(' ', '_')}"
        print(f"{Colors.CYAN}[DEV MODE] Using mock directory: {mock_path}{Colors.RESET}")
        return mock_path
    
    config_key = f"source_folder_{data_type.lower().replace(' ', '_')}"
    saved_source = load_config(config_key)

    if saved_source and os.path.isdir(saved_source):
        print(f"\n{Colors.CYAN}CURRENT {data_type} DIRECTORY: {saved_source}{Colors.RESET}")
        choice = input(f"PRESS{Colors.YELLOW} ENTER{Colors.RESET} TO KEEP, OR PASTE A NEW DIRECTORY: ").strip()
        
        if choice:
            while not os.path.isdir(choice):
                print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
                choice = input("Paste directory: ").strip()
            source_folder = choice
        else:
            source_folder = saved_source
    else:
        source_folder = input(f"Paste {data_type} directory: ").strip()
        while not os.path.isdir(source_folder):
            print(f"{Colors.RED}INVALID PATH. PLEAE ENTER A VALID DIRECTORY.{Colors.RESET}")
            source_folder = input("Paste directory: ").strip()
    
    save_config(config_key, source_folder)
    print(f"{Colors.GREEN}{data_type} folder set to: {source_folder}{Colors.RESET}")
    logging.info(f"{data_type} directory set: {source_folder}")
    return source_folder

def create_directories(base_path: str, structure: Dict) -> None:
    """
    Recursively create directory structure.
    
    Args:
        base_path: Base path for directory creation
        structure: Dictionary defining the directory structure
    """
    try:
        os.makedirs(base_path, exist_ok=True)
        for name, content in structure.items():
            path = os.path.join(base_path, name)
            if isinstance(content, dict):
                create_directories(path, content)
            elif isinstance(content, list):
                os.makedirs(path, exist_ok=True)
                for subdir in content:
                    os.makedirs(os.path.join(path, subdir), exist_ok=True)
            else:
                os.makedirs(path, exist_ok=True)
    except (OSError, PermissionError) as e:
        logging.error(f"Error creating directories at {base_path}: {e}")
        raise

def create_folder_structure(parent_folder: str) -> None:
    """
    Create folder structure for TERN plot IDs.
    
    Args:
        parent_folder: Parent directory path
    """
    print(f"\n{Colors.GREEN}{Colors.BOLD}CREATE FOLDER STRUCTURE{Colors.RESET}")
    print(f"{'─' * 90}")
    print(f"\n{Colors.CYAN}ENTER A TERN PLOT ID TO GENERATE THE FOLDER STRUCTURE (E.G. WAAPIL0013){Colors.RESET}")
    print(f"TYPE {Colors.YELLOW}'FINISHED'{Colors.RESET} WHEN ALL DESIRED PLOT ID's HAVE BEEN ADDED.\n")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating folder creation{Colors.RESET}")
        time.sleep(1)
        print(f"{Colors.GREEN}Folder creation complete!{Colors.RESET}")
        return
    
    today_str = date.today().strftime("%Y%m%d")
    created_count = 0
    
    while True:
        site_name = input("\nENTER TERN PLOT ID: ").strip().upper()
        
        if site_name == "FINISHED":
            print(f"\n{Colors.GREEN}FOLDER GENERATION COMPLETED! YOU HAVE GENERATED {created_count} SITE(S).{Colors.RESET}")
            logging.info(f"Created {created_count} site folder structures")
            break
        
        if not site_name:
            continue
            
        if site_name not in SITE_DATABASE:
            print(f"{Colors.RED}ERROR: PLOT ID '{site_name}' NOT FOUND IN DATABASE. PLEASE RE-ENTER SITE ID.{Colors.RESET}")
            logging.warning(f"INVALID POT ID ATTEMPTED: {site_name}")
            continue
        
        site_path = os.path.join(parent_folder, site_name)
        if os.path.exists(site_path):
            print(f"{Colors.RED}ERROR: SITE '{site_name}' ALREADY EXISTS AT {site_path}{Colors.RESET}")
            continue
        
        structure = {
            today_str: {
                "imagery": {"rgb": ["level0_raw"], "multispec": ["level0_raw"]},
                "metadata": {},
                "lidar": ["level0_raw"],
                "drtk": {},
                "b-roll": {},
            }
        }
        
        try:
            create_directories(site_path, structure)
            print(f"{Colors.GREEN}✓ Folder structure created for site '{site_name}'{Colors.RESET}")
            logging.info(f"Created folder structure for {site_name}")
            created_count += 1
        except Exception as e:
            print(f"{Colors.RED}✗ Error creating structure for '{site_name}': {e}{Colors.RESET}")
            logging.error(f"Failed to create structure for {site_name}: {e}")

print("DEBUG: Directory management defined")

# ============================================================================
# FILE TRANSFER OPERATIONS
# ============================================================================

print("DEBUG: Defining file transfer operations...")

def copy_with_speed(src: str, dst: str) -> Tuple[float, float]:
    """
    Copy directory with real-time speed display and transfer time.
    
    Args:
        src: Source directory path
        dst: Destination directory path
        
    Returns:
        Tuple of (average_speed, transfer_time)
    """
    total_size = get_folder_size(src)
    copied_size = 0
    start_time = time.time()
    
    try:
        for root, dirs, files in os.walk(src):
            rel_path = os.path.relpath(root, src)
            dst_root = os.path.join(dst, rel_path)
            os.makedirs(dst_root, exist_ok=True)
            
            for filename in files:
                src_file = os.path.join(root, filename)
                dst_file = os.path.join(dst_root, filename)
                
                file_size = os.path.getsize(src_file)
                shutil.copy2(src_file, dst_file)
                
                copied_size += file_size
                elapsed = time.time() - start_time
                speed = copied_size / elapsed if elapsed > 0 else 0
                progress = (copied_size / total_size) * 100 if total_size > 0 else 0
                
                print(f"\r{Colors.PURPLE}Progress: {progress:.1f}% | Speed: {format_speed(speed)} | {format_size(copied_size)}/{format_size(total_size)}{Colors.RESET}", end='', flush=True)
        
        transfer_time = time.time() - start_time
        avg_speed = copied_size / transfer_time if transfer_time > 0 else 0
        print()  # New line after completion
        return avg_speed, transfer_time
        
    except (OSError, IOError, PermissionError) as e:
        logging.error(f"Error during copy from {src} to {dst}: {e}")
        raise

def transfer_lidar_data() -> None:
    """Transfer LiDAR (L2) data from SD card to organized structure."""
    print_part_header(3, "L2 (LiDAR) DATA TRANSFER")
    
    print(f"{Colors.CYAN}")
    print(f""" 
    TO BEGIN STEP 3 PLEASE ASSIGN AN L2 SD CARD DIRTECTORY, THIS CAN BE DONE BY COPYING
    THE FILE PATH OF THE SD CARD READER AND PASTING IT BELOW.
    
    ONCE COMPLETED THE TRANSFER OF L2 DATA WILL COMMENSE AUTOMATICALLY ONCE THE {Colors.YELLOW}'PROCEED'{Colors.RESET} {Colors.CYAN}PROMPT IS ENTERED.

    ONCE THE TRANSFER IS COMPLETE A SUMMARY TABLE WILL INDICATE THE SUCCESS OR FAILURE OF THE TRANSFER(S).

    ENSURE TO DOUBLE CHECK THAT THE FILES OUTPUTTED TO THE CORRECT SITE FOLDER.{Colors.RESET}""")
    
    source_folder = get_source_directory("L2 SD CARD")
    base_target_dir = load_config("parent_folder")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating L2 transfer{Colors.RESET}")
        time.sleep(2)
        return
    
    # Count folders first
    folder_count = 0
    for root, dirs, files in os.walk(source_folder):
        if root == source_folder:
            folder_count = len(dirs)
            break
    
    if folder_count == 0:
        print(f"{Colors.YELLOW}NO FOLDERS FOUND IN SOURCE DIRECTORY.{Colors.RESET}")
        return
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}╔{'═' * 88}╗")
    print(f"║{'FOLDERS IN L2 SD CARD DIRECTORY':^88}║")
    print(f"╚{'═' * 88}╝{Colors.RESET}")
    
    for root, dirs, files in os.walk(source_folder):
        if root == source_folder:
            for d in dirs:
                print(f"  {Colors.CYAN}•{Colors.RESET} {d}")
    
    print(f"\n{Colors.YELLOW}ABOVE ARE THE {folder_count} L2 FOLDER(S) TO BE COPIED.")
    print(f"CHECK THEY MATCH YOUR EXPECTED SITE IDs.{Colors.RESET}")
    proceed_prompt()
    
    today_str = datetime.today().strftime('%Y%m%d')
    summary = []
    
    for root, dirs, files in os.walk(source_folder):
        if files:
            folder_name = os.path.basename(root)
            num_files = len(files)
            folder_size = get_folder_size(root)
            folder_size_gb = folder_size / (1024 ** 3)
            
            print(f"\n{Colors.CYAN}Processing: {folder_name}{Colors.RESET}")
            print(f"Files: {num_files} | Size: {format_size(folder_size)}")
            
            present_extensions = {f.split('.')[-1].upper() for f in files if '.' in f}
            missing = [ext for ext in LIDAR_REQUIRED_FILES if ext not in present_extensions]
            
            if missing:
                print(f"{Colors.RED}✗ Missing required file types: {', '.join(missing)}{Colors.RESET}")
                copy_status = f"Missing: {', '.join(missing)}"
                plot_id = "UNKNOWN"
                logging.warning(f"L2 folder {folder_name} missing files: {missing}")
            else:
                print(f"{Colors.GREEN}✓ All required file types present{Colors.RESET}")
                
                try:
                    plot_id = folder_name.split('-')[-2]
                except IndexError:
                    plot_id = "UNKNOWN"
                    logging.warning(f"Cannot extract plot ID from {folder_name}")
                
                target_path = os.path.join(base_target_dir, plot_id, today_str, "lidar", "level0_raw")
                os.makedirs(target_path, exist_ok=True)
                dest_folder = os.path.join(target_path, folder_name)
                
                try:
                    if os.path.exists(dest_folder):
                        print(f"{Colors.YELLOW}Already exists, skipping copy{Colors.RESET}")
                        copy_status = "Already exists"
                    else:
                        avg_speed, transfer_time = copy_with_speed(root, dest_folder)
                        print(f"{Colors.GREEN}✓ Successfully copied to {dest_folder}{Colors.RESET}")
                        print(f"{Colors.GREEN}Transfer time: {format_time(transfer_time)} | Avg speed: {format_speed(avg_speed)}{Colors.RESET}")
                        copy_status = "Copied"
                        logging.info(f"Copied L2 data: {folder_name} -> {dest_folder}")
                except Exception as e:
                    print(f"{Colors.RED}✗ Error copying: {e}{Colors.RESET}")
                    copy_status = f"Error: {str(e)[:30]}"
                    logging.error(f"Failed to copy {folder_name}: {e}")
            
            summary.append({
                "Folder": folder_name, 
                "Plot ID": plot_id, 
                "Files": num_files, 
                "Size (GB)": f"{folder_size_gb:.2f}", 
                "Copy Status": copy_status
            })
    
    # Print summary table
    print(f"\n\n{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
    print("║                                  TRANSFER SUMMARY                                      ║")
    print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
    print(f"{Colors.RESET}")
    header = f"{Colors.BOLD}{'Folder':25} | {'Plot ID':10} | {'Files':5} | {'Size (GB)':8} | {'Status':20}{Colors.RESET}"
    print(header)
    print("─" * 90)
    
    for item in summary:
        print(f"{item['Folder'][:25]:25} | {item['Plot ID']:10} | {item['Files']:5} | {item['Size (GB)']:8} | {color_status(item['Copy Status'])}")
    
    print(f"{Colors.BOLD}")
    print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")
    
    print_completion_banner("L2 DATA TRANSFER COMPLETE")

def transfer_p1_rgb_data() -> None:
    """Transfer P1 RGB imagery data from SD card to organized structure."""
    print_part_header(4, "P1 (RGB IMAGERY) DATA TRANSFER")
    
    print(f"\n{Colors.CYAN}THE P1 DATA TRANSFER WILL BE COMPLETED AUTOMATICALLY ONCE THE {Colors.YELLOW}'PROCEED'{Colors.RESET}{Colors.CYAN} COMMAND IS ENTERED.")
    print(f"THE DATA WILL BE QUALITY CONTROLLED ONCE TRANSFER IS COMPLETE.")
    print(f"ENSURE TO DOUBLE CHECK THAT THE OUTPUT IS AS DESIRED.{Colors.RESET}\n")
    
    source_folder = get_source_directory("P1 SD CARD")
    base_target_dir = load_config("parent_folder")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating P1 transfer{Colors.RESET}")
        time.sleep(2)
        return
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}╔{'═' * 88}╗")
    print(f"║{'FOLDERS IN P1 SD CARD DIRECTORY':^88}║")
    print(f"╚{'═' * 88}╝{Colors.RESET}")
    
    p1_found = False
    for root, dirs, files in os.walk(source_folder):
        if root == source_folder:
            for d in dirs:
                if "-P1" in d:
                    print(f"  {Colors.CYAN}•{Colors.RESET} {d}")
                    p1_found = True
    
    if not p1_found:
        print(f"{Colors.YELLOW}No P1 folders found (folders must contain '-P1' in name){Colors.RESET}")
        logging.warning("No P1 folders found in source directory")
        return
    
    print(f"\n{Colors.YELLOW}ABOVE ARE THE P1 FOLDERS TO BE COPIED.")
    print(f"CHECK THEY MATCH YOUR EXPECTED SITE IDs.{Colors.RESET}")
    proceed_prompt()
    
    today_str = datetime.today().strftime('%Y%m%d')
    summary = []
    
    for root, dirs, files in os.walk(source_folder):
        if files and "-P1" in os.path.basename(root):
            folder_name = os.path.basename(root)
            num_files = len(files)
            folder_size = get_folder_size(root)
            folder_size_gb = folder_size / (1024 ** 3)
            
            creation_time = datetime.fromtimestamp(os.path.getctime(root)).strftime("%Y-%m-%d %H:%M:%S")
            modified_time = datetime.fromtimestamp(os.path.getmtime(root)).strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"\n{Colors.CYAN}Processing: {folder_name}{Colors.RESET}")
            print(f"Created: {creation_time} | Modified: {modified_time}")
            print(f"Files: {num_files} | Size: {format_size(folder_size)}")
            
            present_extensions = {f.split('.')[-1].upper() for f in files if '.' in f}
            missing = [ext for ext in P1_REQUIRED_FILES if ext not in present_extensions]
            
            if missing:
                print(f"{Colors.RED}✗ Missing required file types: {', '.join(missing)}{Colors.RESET}")
                copy_status = f"Missing: {', '.join(missing)}"
                plot_id = "UNKNOWN"
                logging.warning(f"P1 folder {folder_name} missing files: {missing}")
            else:
                print(f"{Colors.GREEN}✓ All required file types present{Colors.RESET}")
                
                try:
                    plot_id = folder_name.split('-')[-2]
                except IndexError:
                    print(f"{Colors.RED}Cannot extract TERN Plot ID from {folder_name}{Colors.RESET}")
                    plot_id = "UNKNOWN"
                    logging.warning(f"Cannot extract plot ID from {folder_name}")
                
                target_path = os.path.join(base_target_dir, plot_id, today_str, "imagery", "rgb", "level0_raw")
                os.makedirs(target_path, exist_ok=True)
                dest_folder = os.path.join(target_path, folder_name)
                
                try:
                    if os.path.exists(dest_folder):
                        print(f"{Colors.YELLOW}Already exists, skipping copy{Colors.RESET}")
                        copy_status = "Already exists"
                    else:
                        avg_speed, transfer_time = copy_with_speed(root, dest_folder)
                        print(f"{Colors.GREEN}✓ Successfully copied to {dest_folder}{Colors.RESET}")
                        print(f"{Colors.GREEN}Transfer time: {format_time(transfer_time)} | Avg speed: {format_speed(avg_speed)}{Colors.RESET}")
                        copy_status = "Copied"
                        logging.info(f"Copied P1 data: {folder_name} -> {dest_folder}")
                except Exception as e:
                    print(f"{Colors.RED}✗ Error copying: {e}{Colors.RESET}")
                    copy_status = f"Error: {str(e)[:30]}"
                    logging.error(f"Failed to copy {folder_name}: {e}")
            
            summary.append({
                "Folder": folder_name, 
                "Plot ID": plot_id, 
                "Files": num_files,
                "Size (GB)": f"{folder_size_gb:.2f}", 
                "Copy Status": copy_status
            })
    
    # Print summary table
    print(f"\n\n{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
    print("║                                  TRANSFER SUMMARY                                      ║")
    print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
    print(f"{Colors.RESET}")
    header = f"{Colors.BOLD}{'Folder':30} | {'Plot ID':10} | {'Files':5} | {'Size (GB)':8} | {'Status':20}{Colors.RESET}"
    print(header)
    print("─" * 90)
    
    for item in summary:
        print(f"{item['Folder'][:30]:30} | {item['Plot ID']:10} | {item['Files']:5} | {item['Size (GB)']:8} | {color_status(item['Copy Status'])}")
    
    print(f"{Colors.BOLD}")
    print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")
    
    print_completion_banner("P1 RGB DATA TRANSFER COMPLETE")

print("DEBUG: File transfer operations defined")

# ============================================================================
# MICASENSE CAMERA DETECTION
# ============================================================================

print("DEBUG: Defining MicaSense camera detection...")

def detect_micasense_camera(tif_path: str) -> Optional[str]:
    """
    Detect if a TIF file is from RED or BLUE MicaSense camera.
    
    Args:
        tif_path: Path to the TIF file
        
    Returns:
        "RED", "BLUE", or None if cannot determine
    """
    filename = os.path.basename(tif_path)
    
    # Method 1: Check filename pattern (_1 to _6 = RED, _7 to _11 = BLUE)
    if "_" in filename:
        try:
            # Extract number after last underscore and before extension
            name_part = filename.rsplit(".", 1)[0]  # Remove extension
            if "_" in name_part:
                suffix = name_part.split("_")[-1]
                if suffix.isdigit():
                    num = int(suffix)
                    if 1 <= num <= 6:
                        return "RED"
                    elif 7 <= num <= 11:
                        return "BLUE"
        except (ValueError, IndexError):
            pass
    
    # Method 2: Check EXIF data for camera serial number
    if PIL_AVAILABLE:
        try:
            with Image.open(tif_path) as img:
                exif_data = img.getexif()
                if exif_data:
                    # Search for camera serial in EXIF data
                    for tag_id, value in exif_data.items():
                        tag_name = TAGS.get(tag_id, tag_id)
                        value_str = str(value)
                        
                        if MICASENSE_RED_SERIAL in value_str:
                            return "RED"
                        elif MICASENSE_BLUE_SERIAL in value_str:
                            return "BLUE"
        except Exception as e:
            logging.warning(f"Could not read EXIF from {tif_path}: {e}")
    
    return None

def transfer_micasense_data() -> None:
    """Transfer MicaSense multispectral imagery data from SD card to organized structure."""
    print_part_header(5, "MICASENSE (MULTISPECTRAL IMAGERY) DATA TRANSFER")
    
    print(f"\n{Colors.CYAN}THE MICASENSE DATA TRANSFER WILL MERGE RED AND BLUE CAMERA FILES.")
    print(f"FILES WILL BE REORGANIZED INTO FOLDERS WITH MAX 2200 TIF FILES.")
    print(f"COMPLETE IMAGE SERIES (1-11) WILL BE KEPT TOGETHER.")
    print(f"THE TRANSFER WILL COMMENSE AUTOMATICALLY ONCE THE {Colors.YELLOW}'PROCEED'{Colors.RESET}{Colors.CYAN} COMMAND IS ENTERED.")
    print(f"ENSURE TO DOUBLE CHECK THAT THE OUTPUT IS AS DESIRED.{Colors.RESET}\n")
    
    # Get both RED and BLUE source directories
    print(f"{Colors.GREEN}{Colors.BOLD}FIRST, SET THE RED CAMERA SD CARD DIRECTORY{Colors.RESET}")
    red_source_folder = get_source_directory("MICASENSE RED SD CARD")
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}NOW, SET THE BLUE CAMERA SD CARD DIRECTORY{Colors.RESET}")
    blue_source_folder = get_source_directory("MICASENSE BLUE SD CARD")
    
    base_target_dir = load_config("parent_folder")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating MicaSense transfer{Colors.RESET}")
        time.sleep(2)
        return
    
    # Scan for numbered folders in RED camera
    red_folders = []
    try:
        for item in os.listdir(red_source_folder):
            item_path = os.path.join(red_source_folder, item)
            if os.path.isdir(item_path) and item.isdigit():
                red_folders.append(item)
    except Exception as e:
        print(f"{Colors.RED}Error scanning RED source directory: {e}{Colors.RESET}")
        logging.error(f"Error scanning MicaSense RED source: {e}")
        return
    
    # Scan for numbered folders in BLUE camera
    blue_folders = []
    try:
        for item in os.listdir(blue_source_folder):
            item_path = os.path.join(blue_source_folder, item)
            if os.path.isdir(item_path) and item.isdigit():
                blue_folders.append(item)
    except Exception as e:
        print(f"{Colors.RED}Error scanning BLUE source directory: {e}{Colors.RESET}")
        logging.error(f"Error scanning MicaSense BLUE source: {e}")
        return
    
    red_folders.sort()
    blue_folders.sort()
    
    # Get all unique folder numbers from both cameras
    all_folders = sorted(set(red_folders + blue_folders))
    
    # Now scan for SET folders (0000SET, 0001SET, etc.) that contain the numbered folders
    red_set_folders = {}  # {set_name: [list of numbered folders]}
    blue_set_folders = {}
    
    # Scan RED camera for SET folders
    try:
        for item in os.listdir(red_source_folder):
            item_path = os.path.join(red_source_folder, item)
            if os.path.isdir(item_path) and item.endswith('SET'):
                # Get numbered folders inside this SET
                numbered_in_set = []
                for sub_item in os.listdir(item_path):
                    sub_path = os.path.join(item_path, sub_item)
                    if os.path.isdir(sub_path) and sub_item.isdigit():
                        numbered_in_set.append(sub_item)
                if numbered_in_set:
                    red_set_folders[item] = sorted(numbered_in_set)
    except Exception as e:
        logging.error(f"Error scanning RED SET folders: {e}")
    
    # Scan BLUE camera for SET folders
    try:
        for item in os.listdir(blue_source_folder):
            item_path = os.path.join(blue_source_folder, item)
            if os.path.isdir(item_path) and item.endswith('SET'):
                # Get numbered folders inside this SET
                numbered_in_set = []
                for sub_item in os.listdir(item_path):
                    sub_path = os.path.join(item_path, sub_item)
                    if os.path.isdir(sub_path) and sub_item.isdigit():
                        numbered_in_set.append(sub_item)
                if numbered_in_set:
                    blue_set_folders[item] = sorted(numbered_in_set)
    except Exception as e:
        logging.error(f"Error scanning BLUE SET folders: {e}")
    
    # Get all unique SET folders
    all_set_folders = sorted(set(list(red_set_folders.keys()) + list(blue_set_folders.keys())))
    
    if not all_set_folders:
        print(f"{Colors.YELLOW}No SET folders (0000SET, 0001SET, etc.) found in source directories.{Colors.RESET}")
        return
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}╔{'═' * 88}╗")
    print(f"║{'SET FOLDERS FOUND IN MICASENSE SD CARDS':^88}║")
    print(f"╚{'═' * 88}╝{Colors.RESET}")
    
    print(f"\n{Colors.RED}RED camera SET folders: {len(red_set_folders)}{Colors.RESET}")
    for set_folder in sorted(red_set_folders.keys())[:10]:
        sub_folders = ", ".join(red_set_folders[set_folder])
        print(f"  {Colors.RED}•{Colors.RESET} {set_folder} (contains: {sub_folders})")
    if len(red_set_folders) > 10:
        print(f"  {Colors.RED}... and {len(red_set_folders) - 10} more SET folders{Colors.RESET}")
    
    print(f"\n{Colors.CYAN}BLUE camera SET folders: {len(blue_set_folders)}{Colors.RESET}")
    for set_folder in sorted(blue_set_folders.keys())[:10]:
        sub_folders = ", ".join(blue_set_folders[set_folder])
        print(f"  {Colors.CYAN}•{Colors.RESET} {set_folder} (contains: {sub_folders})")
    if len(blue_set_folders) > 10:
        print(f"  {Colors.CYAN}... and {len(blue_set_folders) - 10} more SET folders{Colors.RESET}")
    
    print(f"\n{Colors.YELLOW}TOTAL UNIQUE SET FOLDERS TO PROCESS: {len(all_set_folders)}{Colors.RESET}")
    
    today_str = datetime.today().strftime('%Y%m%d')
    
    # Group folders and ask for site code for each SET
    print(f"\n{Colors.GREEN}{Colors.BOLD}SET FOLDER TO SITE CODE ASSIGNMENT{Colors.RESET}")
    print(f"{'─' * 90}")
    print(f"{Colors.CYAN}You will now assign a TERN Plot ID to each SET folder.")
    print(f"Each SET contains multiple numbered folders (000, 001, 002, etc.).{Colors.RESET}\n")
    
    if not proceed_prompt("READY TO BEGIN SET FOLDER ASSIGNMENT?"):
        print(f"{Colors.YELLOW}Returning to previous step...{Colors.RESET}")
        return  # Exit function to go back
    
    # Dictionary to store SET folder -> plot_id mapping
    set_assignments = {}
    set_index = 0
    
    while set_index < len(all_set_folders):
        set_folder = all_set_folders[set_index]
        # Get numbered folders in this SET from both cameras
        red_numbered = red_set_folders.get(set_folder, [])
        blue_numbered = blue_set_folders.get(set_folder, [])
        all_numbered = sorted(set(red_numbered + blue_numbered))
        
        # Count total files in this SET
        total_red = 0
        total_blue = 0
        
        for num_folder in all_numbered:
            # Count RED files
            if set_folder in red_set_folders and num_folder in red_set_folders[set_folder]:
                red_path = os.path.join(red_source_folder, set_folder, num_folder)
                if os.path.exists(red_path):
                    total_red += len([f for f in os.listdir(red_path) 
                                    if f.upper().endswith(('.TIF', '.TIFF'))])
            
            # Count BLUE files
            if set_folder in blue_set_folders and num_folder in blue_set_folders[set_folder]:
                blue_path = os.path.join(blue_source_folder, set_folder, num_folder)
                if os.path.exists(blue_path):
                    total_blue += len([f for f in os.listdir(blue_path) 
                                     if f.upper().endswith(('.TIF', '.TIFF'))])
        
        print(f"\n{Colors.CYAN}SET Folder: {Colors.BOLD}{set_folder}{Colors.RESET} ({set_index + 1}/{len(all_set_folders)})")
        print(f"  Contains numbered folders: {', '.join(all_numbered)}")
        print(f"  {Colors.RED}RED camera: {total_red} files{Colors.RESET}")
        print(f"  {Colors.CYAN}BLUE camera: {total_blue} files{Colors.RESET}")
        print(f"  {Colors.YELLOW}Total: {total_red + total_blue} files{Colors.RESET}")
        
        plot_id = input(f"{Colors.YELLOW}TYPE SITE CODE TO TRANSFER THIS SET TO (or 'BACK' to go to previous SET): {Colors.RESET}").strip().upper()
        
        # Check for BACK command
        if plot_id == "BACK":
            if set_index > 0:
                set_index -= 1
                # Remove previous assignment
                prev_set = all_set_folders[set_index]
                if prev_set in set_assignments:
                    del set_assignments[prev_set]
                print(f"{Colors.YELLOW}Going back to previous SET...{Colors.RESET}")
                continue
            else:
                print(f"{Colors.YELLOW}Already at first SET. Cannot go back further.{Colors.RESET}")
                continue
        
        while plot_id not in SITE_DATABASE:
            print(f"{Colors.RED}INVALID PLOT ID '{plot_id}'. PLEASE ENTER A VALID TERN PLOT ID.{Colors.RESET}")
            plot_id = input(f"{Colors.YELLOW}TYPE SITE CODE (or 'BACK'): {Colors.RESET}").strip().upper()
            
            if plot_id == "BACK":
                break
        
        if plot_id == "BACK":
            if set_index > 0:
                set_index -= 1
                prev_set = all_set_folders[set_index]
                if prev_set in set_assignments:
                    del set_assignments[prev_set]
                print(f"{Colors.YELLOW}Going back to previous SET...{Colors.RESET}")
            continue
        
        set_assignments[set_folder] = {
            'plot_id': plot_id,
            'numbered_folders': all_numbered
        }
        print(f"{Colors.GREEN}✓ SET {set_folder} assigned to {plot_id}{Colors.RESET}")
        set_index += 1
    
    # Show assignment summary
    print(f"\n\n{Colors.GREEN}{Colors.BOLD}╔{'═' * 88}╗")
    print(f"║{'SET FOLDER ASSIGNMENT SUMMARY':^88}║")
    print(f"╚{'═' * 88}╝{Colors.RESET}\n")
    
    # Group by plot_id for summary
    plot_id_groups = {}
    for set_folder, assignment in set_assignments.items():
        plot_id = assignment['plot_id']
        if plot_id not in plot_id_groups:
            plot_id_groups[plot_id] = []
        plot_id_groups[plot_id].append(set_folder)
    
    for plot_id, sets in sorted(plot_id_groups.items()):
        set_list = ", ".join(sorted(sets))
        print(f"{Colors.CYAN}{plot_id}:{Colors.RESET} SET folders: {set_list}")
    
    if not proceed_prompt("\nCONFIRM ASSIGNMENTS AND BEGIN TRANSFER?"):
        print(f"{Colors.YELLOW}Restarting SET folder assignment...{Colors.RESET}")
        time.sleep(1)
        # Recursively call the function to restart assignment
        return transfer_micasense_data()
    
    print(f"\n{Colors.GREEN}Starting file transfer and reorganization...{Colors.RESET}\n")
    
    # Collect all TIF files from both cameras with their full paths, organized by plot_id
    files_by_plot = {}
    
    for set_folder, assignment in set_assignments.items():
        plot_id = assignment['plot_id']
        numbered_folders = assignment['numbered_folders']
        
        if plot_id not in files_by_plot:
            files_by_plot[plot_id] = []
        
        # Process each numbered folder within this SET
        for num_folder in numbered_folders:
            # Get TIF files from RED camera
            red_folder_path = os.path.join(red_source_folder, set_folder, num_folder)
            if os.path.exists(red_folder_path):
                red_files = [f for f in os.listdir(red_folder_path) 
                            if f.upper().endswith(('.TIF', '.TIFF'))]
                for f in red_files:
                    files_by_plot[plot_id].append({
                        'filename': f,
                        'full_path': os.path.join(red_folder_path, f),
                        'camera': 'RED',
                        'source_set': set_folder,
                        'source_folder': num_folder
                    })
            
            # Get TIF files from BLUE camera
            blue_folder_path = os.path.join(blue_source_folder, set_folder, num_folder)
            if os.path.exists(blue_folder_path):
                blue_files = [f for f in os.listdir(blue_folder_path) 
                             if f.upper().endswith(('.TIF', '.TIFF'))]
                for f in blue_files:
                    files_by_plot[plot_id].append({
                        'filename': f,
                        'full_path': os.path.join(blue_folder_path, f),
                        'camera': 'BLUE',
                        'source_set': set_folder,
                        'source_folder': num_folder
                    })
    
    # Process each plot_id separately
    all_summaries = {}
    grand_total_copied = 0
    grand_total_skipped = 0
    
    for plot_id in sorted(files_by_plot.keys()):
        all_tif_files = files_by_plot[plot_id]
        
        # Sort all files by filename to maintain chronological order
        all_tif_files.sort(key=lambda x: x['filename'])
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}═══════════════════════════════════════════════════════════════════════════════════════")
        print(f"PROCESSING PLOT: {plot_id}")
        print(f"═══════════════════════════════════════════════════════════════════════════════════════{Colors.RESET}")
        print(f"{Colors.CYAN}Total TIF files for this plot: {len(all_tif_files)}{Colors.RESET}")
        print(f"{Colors.CYAN}Reorganizing into folders with max 2200 files (keeping series 1-11 together)...{Colors.RESET}\n")
    
    # Reorganize into folders with max 2200 files, keeping complete series together
    MAX_FILES_PER_FOLDER = 2200
    current_folder_num = 0
    current_folder_count = 0
    summary = []
    total_copied = 0
    total_skipped = 0
    
    i = 0
    while i < len(all_tif_files):
        file_info = all_tif_files[i]
        filename = file_info['filename']
        
        # Extract base name (IMG_0000) and series number (_1 to _11)
        try:
            # Handle different naming patterns
            name_without_ext = filename.rsplit('.', 1)[0]
            
            # Check if filename has underscore pattern
            if '_' in name_without_ext:
                parts = name_without_ext.rsplit('_', 1)
                base_name = parts[0]
                series_num = int(parts[1]) if parts[1].isdigit() else None
            else:
                base_name = name_without_ext
                series_num = None
        except (ValueError, IndexError):
            base_name = name_without_ext
            series_num = None
        
        # Count how many files in this series (1-11 or 1-6 for RED only, 7-11 for BLUE only)
        series_count = 0
        if series_num is not None:
            # Look ahead to count complete series
            for j in range(i, min(i + 11, len(all_tif_files))):
                check_file = all_tif_files[j]['filename']
                check_name = check_file.rsplit('.', 1)[0]
                if '_' in check_name and check_name.rsplit('_', 1)[0] == base_name:
                    series_count += 1
                else:
                    break
        else:
            series_count = 1
        
        # Check if adding this complete series would exceed folder limit
        if current_folder_count > 0 and (current_folder_count + series_count) > MAX_FILES_PER_FOLDER:
            # Move to next folder
            print(f"{Colors.GREEN}✓ Folder {str(current_folder_num).zfill(3)} completed with {current_folder_count} files{Colors.RESET}")
            summary.append({
                'Folder': str(current_folder_num).zfill(3),
                'Files': current_folder_count
            })
            current_folder_num += 1
            current_folder_count = 0
        
        # Create target directory
        target_folder = str(current_folder_num).zfill(3)
        target_path = os.path.join(base_target_dir, plot_id, today_str, 
                                   "imagery", "multispec", "level0_raw", target_folder)
        os.makedirs(target_path, exist_ok=True)
        
        # Copy the complete series
        for j in range(i, min(i + series_count, len(all_tif_files))):
            file_to_copy = all_tif_files[j]
            src = file_to_copy['full_path']
            dst = os.path.join(target_path, file_to_copy['filename'])
            
            try:
                if os.path.exists(dst):
                    total_skipped += 1
                else:
                    shutil.copy2(src, dst)
                    total_copied += 1
                current_folder_count += 1
            except Exception as e:
                logging.error(f"Failed to copy file {file_to_copy['filename']}: {e}")
                print(f"  {Colors.RED}✗ Error copying {file_to_copy['filename']}: {e}{Colors.RESET}")
        
        # Move index forward by series count
        i += series_count
        
        # Show progress every 100 files
        if total_copied % 100 == 0 and total_copied > 0:
            print(f"{Colors.PURPLE}Progress: {total_copied} files copied...{Colors.RESET}")
    
    # Add final folder to summary
    if current_folder_count > 0:
        print(f"{Colors.GREEN}✓ Folder {str(current_folder_num).zfill(3)} completed with {current_folder_count} files{Colors.RESET}")
        summary.append({
            'Folder': str(current_folder_num).zfill(3),
            'Files': current_folder_count
        })
    
    # Store this plot's summary
    all_summaries[plot_id] = {
        'folders': summary,
        'total_copied': total_copied,
        'total_skipped': total_skipped
    }
    grand_total_copied += total_copied
    grand_total_skipped += total_skipped
    
    # Print summary for this plot
    print(f"\n{Colors.GREEN}Plot {plot_id} Summary: {total_copied} files copied into {len(summary)} folders{Colors.RESET}")
    
    # Print combined summary table for all plots
    print(f"\n\n{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
    print("║                            COMBINED TRANSFER SUMMARY                                   ║")
    print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
    print(f"{Colors.RESET}")
    print(f"Total plots processed: {len(all_summaries)}")
    print(f"Total files copied: {grand_total_copied}")
    if grand_total_skipped > 0:
        print(f"{Colors.YELLOW}Total files skipped (already exist): {grand_total_skipped}{Colors.RESET}")
    print("─" * 90)
    
    for plot_id in sorted(all_summaries.keys()):
        plot_summary = all_summaries[plot_id]
        print(f"\n{Colors.CYAN}{Colors.BOLD}Plot ID: {plot_id}{Colors.RESET}")
        print(f"  Output folders created: {len(plot_summary['folders'])}")
        print(f"  Files copied: {plot_summary['total_copied']}")
        
        header = f"  {Colors.BOLD}{'Folder':15} | {'Files':10} | {'Status':20}{Colors.RESET}"
        print(f"\n{header}")
        print("  " + "─" * 50)
        
        for item in plot_summary['folders'][:10]:  # Show first 10 folders per plot
            status = f"{Colors.GREEN}✓ Complete{Colors.RESET}" if item['Files'] <= MAX_FILES_PER_FOLDER else f"{Colors.YELLOW}Over limit{Colors.RESET}"
            print(f"  {item['Folder']:15} | {item['Files']:10} | {status}")
        
        if len(plot_summary['folders']) > 10:
            print(f"  {Colors.CYAN}... and {len(plot_summary['folders']) - 10} more folders{Colors.RESET}")
    
    print(f"\n{Colors.BOLD}")
    print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")
    
    print(f"{Colors.GREEN}{Colors.BOLD}✓ SUCCESS: All files have been reorganized with max 2200 files per folder!{Colors.RESET}")
    print(f"{Colors.GREEN}Complete image series (1-11) have been kept together.{Colors.RESET}")
    
    # ============================================================================
    # CORRUPTION CHECK (for all plots)
    # ============================================================================
    
    print(f"\n{Colors.CYAN}{Colors.BOLD}PERFORMING CORRUPTION CHECK ON ALL TRANSFERRED FILES...{Colors.RESET}\n")
    
    corrupted_files = []
    total_files_checked = 0
    
    # Scan all plots
    for plot_id in all_summaries.keys():
        print(f"\n{Colors.CYAN}Checking plot: {plot_id}{Colors.RESET}")
        target_base = os.path.join(base_target_dir, plot_id, today_str, "imagery", "multispec", "level0_raw")
        
        if os.path.exists(target_base):
            for output_folder in sorted(os.listdir(target_base)):
                folder_path = os.path.join(target_base, output_folder)
                if not os.path.isdir(folder_path):
                    continue
                
                print(f"  {Colors.CYAN}Checking folder {output_folder}...{Colors.RESET}", end='', flush=True)
                
                folder_corrupted = []
                for filename in os.listdir(folder_path):
                    if filename.upper().endswith(('.TIF', '.TIFF')):
                        file_path = os.path.join(folder_path, filename)
                        total_files_checked += 1
                        
                        # Check for corruption
                        try:
                            # Check 1: File exists and has size
                            if not os.path.exists(file_path):
                                folder_corrupted.append({
                                    'file': filename,
                                    'folder': output_folder,
                                    'plot': plot_id,
                                    'reason': 'File not found'
                                })
                                continue
                            
                            file_size = os.path.getsize(file_path)
                            if file_size == 0:
                                folder_corrupted.append({
                                    'file': filename,
                                    'folder': output_folder,
                                    'plot': plot_id,
                                    'reason': 'Zero size'
                                })
                                continue
                            
                            # Check 2: Try to read file header
                            with open(file_path, 'rb') as f:
                                header = f.read(4)
                                # TIF files should start with II (little-endian) or MM (big-endian) followed by 42
                                if len(header) < 4:
                                    folder_corrupted.append({
                                        'file': filename,
                                        'folder': output_folder,
                                        'plot': plot_id,
                                        'reason': 'Invalid header (too short)'
                                    })
                                    continue
                                
                                # Check for valid TIFF magic numbers
                                if not ((header[0:2] == b'II' and header[2:4] == b'*\x00') or  # Little-endian
                                        (header[0:2] == b'MM' and header[2:4] == b'\x00*')):    # Big-endian
                                    folder_corrupted.append({
                                        'file': filename,
                                        'folder': output_folder,
                                        'plot': plot_id,
                                        'reason': 'Invalid TIFF header'
                                    })
                                    continue
                            
                            # Check 3: If PIL is available, try to open the image
                            if PIL_AVAILABLE:
                                try:
                                    with Image.open(file_path) as img:
                                        img.verify()  # Verify image integrity
                                except Exception as e:
                                    folder_corrupted.append({
                                        'file': filename,
                                        'folder': output_folder,
                                        'plot': plot_id,
                                        'reason': f'PIL verification failed: {str(e)[:30]}'
                                    })
                                    continue
                        
                        except Exception as e:
                            folder_corrupted.append({
                                'file': filename,
                                'folder': output_folder,
                                'plot': plot_id,
                                'reason': f'Read error: {str(e)[:30]}'
                            })
                
                if folder_corrupted:
                    print(f" {Colors.RED}✗ {len(folder_corrupted)} corrupted{Colors.RESET}")
                    corrupted_files.extend(folder_corrupted)
                else:
                    print(f" {Colors.GREEN}✓ All OK{Colors.RESET}")
    
    # Display corruption report
    if corrupted_files:
        print(f"\n\n{Colors.RED}{Colors.BOLD}")
        print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
        print("║                            CORRUPTION REPORT                                           ║")
        print("║                      ⚠ WARNING: CORRUPTED FILES DETECTED ⚠                            ║")
        print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
        print(f"{Colors.RESET}")
        print(f"{Colors.RED}Total files checked: {total_files_checked}{Colors.RESET}")
        print(f"{Colors.RED}{Colors.BOLD}Total corrupted files: {len(corrupted_files)}{Colors.RESET}")
        print(f"{Colors.YELLOW}NOTE: Corrupted files have been kept in place for manual review.{Colors.RESET}")
        print("─" * 90)
        
        header = f"{Colors.BOLD}{'Plot ID':10} | {'Folder':10} | {'Filename':35} | {'Reason':25}{Colors.RESET}"
        print(f"\n{header}")
        print("─" * 90)
        
        # Show first 50 corrupted files
        for item in corrupted_files[:50]:
            print(f"{item['plot']:10} | {item['folder']:10} | {item['file'][:35]:35} | {Colors.RED}{item['reason'][:25]:25}{Colors.RESET}")
        
        if len(corrupted_files) > 50:
            print(f"\n{Colors.YELLOW}... and {len(corrupted_files) - 50} more corrupted files{Colors.RESET}")
        
        print(f"\n{Colors.RED}{Colors.BOLD}")
        print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
        print(f"{Colors.RESET}\n")
        
        print(f"{Colors.YELLOW}⚠ ACTION REQUIRED:{Colors.RESET}")
        print(f"  1. Review corrupted files manually")
        print(f"  2. Attempt to re-copy from original source if possible")
        print(f"  3. Document corrupted files for data quality report")
        print(f"  4. {Colors.RED}DO NOT DELETE corrupted files - keep for forensic analysis{Colors.RESET}\n")
        
        logging.warning(f"MicaSense corruption check: {len(corrupted_files)} corrupted files found")
    else:
        print(f"\n{Colors.GREEN}{Colors.BOLD}╔════════════════════════════════════════════════════════════════════════════════════════╗")
        print(f"║           ✓ CORRUPTION CHECK PASSED - ALL {total_files_checked} FILES ARE VALID ✓                       ║")
        print(f"╚════════════════════════════════════════════════════════════════════════════════════════╝{Colors.RESET}\n")
        logging.info(f"MicaSense corruption check: All {total_files_checked} files passed validation")
    
    print_completion_banner("MICASENSE DATA TRANSFER COMPLETE")
    
    # Log summary for all plots
    for plot_id, plot_summary in all_summaries.items():
        logging.info(f"MicaSense transfer for {plot_id}: {plot_summary['total_copied']} files copied into {len(plot_summary['folders'])} folders")

print("DEBUG: MicaSense transfer operations defined")

# ============================================================================
# DATA VERIFICATION
# ============================================================================

print("DEBUG: Defining data verification...")

def verify_data_integrity(base_dir_name: str = "primary") -> None:
    """
    Perform comprehensive data integrity verification.
    
    Args:
        base_dir_name: Name of directory to verify ("primary" or "backup")
    """
    print_part_header(
        6 if base_dir_name == "primary" else 8, 
        f"DATA INTEGRITY VERIFICATION ({base_dir_name.upper()})"
    )
    
    section_start_time = time.time()
    
    print(f"\n{Colors.CYAN}")
    print("NOW THAT YOU HAVE COMPLETED THE DATA TRANSFER, THE NEXT STEP IS TO PERFORM".center(90))
    print("A COMPREHENSIVE FOLDER SIZE AND CORRUPTION CHECK ON ALL TRANSFERRED FILES.".center(90))
    print(f"IF ANY ERRORS ARE PRESENT, THEY WILL APPEAR IN {Colors.RED}{Colors.BOLD}RED{Colors.RESET}{Colors.CYAN}.".center(90))
    print(f"{Colors.RESET}\n")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating verification{Colors.RESET}")
        time.sleep(2)
        return
    
    proceed_prompt()
    
    if base_dir_name == "primary":
        base_dir = load_config("parent_folder")
    else:
        base_dir = load_config("backup_folder")
    
    if not base_dir or not os.path.isdir(base_dir):
        print(f"{Colors.RED}Cannot find {base_dir_name} directory. Skipping verification.{Colors.RESET}")
        logging.error(f"Cannot find {base_dir_name} directory: {base_dir}")
        return
    
    print(f"\n{Colors.CYAN}Scanning directory: {base_dir}{Colors.RESET}")
    
    total_sites = 0
    total_files = 0
    total_corrupt = 0
    site_reports = []
    
    for site_folder in os.listdir(base_dir):
        site_path = os.path.join(base_dir, site_folder)
        if not os.path.isdir(site_path):
            continue
        
        total_sites += 1
        print(f"\n{Colors.CYAN}Checking site: {site_folder}{Colors.RESET}")
        
        folder_size = get_folder_size(site_path)
        folder_size_gb = folder_size / (1024 ** 3)
        print(f"Folder size: {format_size(folder_size)}")
        
        corrupt_files = []
        file_count = 0
        
        for root, dirs, files in os.walk(site_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                file_count += 1
                total_files += 1
                
                if check_file_corruption(file_path):
                    relative_path = os.path.relpath(file_path, site_path)
                    corrupt_files.append(relative_path)
                    total_corrupt += 1
        
        if corrupt_files:
            print(f"{Colors.RED}✗ Found {len(corrupt_files)} corrupt file(s):{Colors.RESET}")
            for corrupt_file in corrupt_files[:10]:
                print(f"    - {corrupt_file}")
            if len(corrupt_files) > 10:
                print(f"    ... and {len(corrupt_files) - 10} more")
            status = f"{Colors.RED}ISSUES FOUND{Colors.RESET}"
            logging.warning(f"Site {site_folder} has {len(corrupt_files)} corrupt files")
        else:
            print(f"{Colors.GREEN}✓ All {file_count} files validated successfully{Colors.RESET}")
            status = f"{Colors.GREEN}PASSED{Colors.RESET}"
        
        site_reports.append({
            "Site": site_folder,
            "Files": file_count,
            "Size (GB)": f"{folder_size_gb:.2f}",
            "Corrupt": len(corrupt_files),
            "Status": status
        })
    
    print(f"\n\n{Colors.BOLD}")
    print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
    print("║                                VERIFICATION SUMMARY                                    ║")
    print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
    print(f"{Colors.RESET}")
    print(f"Total sites checked: {total_sites}")
    print(f"Total files scanned: {total_files}")
    print(f"Total corrupt files: {total_corrupt}")
    print("─" * 90)
    
    header = f"{Colors.BOLD}{'Site':20} | {'Files':8} | {'Size (GB)':10} | {'Corrupt':8} | {'Status':15}{Colors.RESET}"
    print(f"\n{header}")
    print("─" * 90)
    
    for report in site_reports:
        print(f"{report['Site'][:20]:20} | {report['Files']:8} | {report['Size (GB)']:10} | {report['Corrupt']:8} | {report['Status']}")
    
    print(f"\n{Colors.BOLD}")
    print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}\n")
    
    if total_corrupt > 0:
        print(f"{Colors.RED}{Colors.BOLD}⚠ WARNING: {total_corrupt} corrupt file(s) detected!{Colors.RESET}")
        print(f"{Colors.YELLOW}Please review the affected files and re-transfer if necessary.{Colors.RESET}")
        logging.warning(f"Verification found {total_corrupt} corrupt files")
    else:
        print(f"{Colors.GREEN}{Colors.BOLD}✓ SUCCESS: All files passed integrity verification!{Colors.RESET}")
        logging.info("All files passed verification")
    
    total_section_time = time.time() - section_start_time
    print_completion_banner(f"DATA INTEGRITY VERIFICATION COMPLETE ({base_dir_name.upper()})", total_section_time)

print("DEBUG: Data verification defined")

# ============================================================================
# BACKUP OPERATIONS
# ============================================================================

print("DEBUG: Defining backup operations...")

def compare_directories(dir1: str, dir2: str) -> Tuple[bool, Set[Tuple[str, str]]]:
    """
    Compare two directories and return detailed differences by folder.
    
    Args:
        dir1: Primary directory path
        dir2: Backup directory path
        
    Returns:
        Tuple of (is_identical, affected_folders)
    """
    print(f"\n{Colors.CYAN}Comparing directories...{Colors.RESET}")
    logging.info(f"Comparing {dir1} with {dir2}")
    
    def get_file_list(directory: str) -> Dict[str, int]:
        """Get dictionary of relative file paths and their sizes."""
        files = {}
        for root, dirs, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                rel_path = os.path.relpath(filepath, directory)
                try:
                    files[rel_path] = os.path.getsize(filepath)
                except OSError:
                    pass
        return files
    
    files1 = get_file_list(dir1)
    files2 = get_file_list(dir2)
    
    only_in_1 = set(files1.keys()) - set(files2.keys())
    only_in_2 = set(files2.keys()) - set(files1.keys())
    common = set(files1.keys()) & set(files2.keys())
    
    size_diff = [f for f in common if files1[f] != files2[f]]
    
    print(f"\nFiles in primary only: {len(only_in_1)}")
    print(f"Files in backup only: {len(only_in_2)}")
    print(f"Size mismatches: {len(size_diff)}")
    
    # Collect affected folders
    affected_folders = set()
    
    for file_path in only_in_1:
        folder = os.path.dirname(file_path).split(os.sep)[0] if os.sep in file_path else file_path
        affected_folders.add((folder, "Missing in backup"))
    
    for file_path in only_in_2:
        folder = os.path.dirname(file_path).split(os.sep)[0] if os.sep in file_path else file_path
        affected_folders.add((folder, "Extra in backup"))
    
    for file_path in size_diff:
        folder = os.path.dirname(file_path).split(os.sep)[0] if os.sep in file_path else file_path
        affected_folders.add((folder, "Size mismatch"))
    
    is_identical = not (only_in_1 or only_in_2 or size_diff)
    
    if is_identical:
        print(f"{Colors.GREEN}✓ Directories are IDENTICAL!{Colors.RESET}")
        logging.info("Directories are identical")
    else:
        print(f"{Colors.YELLOW}⚠ Directories are NOT identical{Colors.RESET}")
        logging.warning(f"Directories differ: {len(affected_folders)} affected folders")
    
    return is_identical, affected_folders

def ssd_backup() -> None:
    """Perform complete SSD backup operation."""
    print("DEBUG: Inside ssd_backup function")
    print_part_header(7, "SSD BACKUP")
    
    section_start_time = time.time()
    
    print(f"{Colors.CYAN}Creating complete backup of primary SSD to backup SSD{Colors.RESET}")
    
    primary_dir = load_config("parent_folder")
    backup_dir = get_source_directory("BACKUP SSD")
    
    if DEV_MODE:
        print(f"{Colors.CYAN}[DEV MODE] Simulating backup{Colors.RESET}")
        time.sleep(2)
        return
    
    proceed_prompt("Ready to begin backup process")
    
    print(f"\n{Colors.CYAN}Starting backup...{Colors.RESET}")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for site_folder in os.listdir(primary_dir):
        site_src = os.path.join(primary_dir, site_folder)
        if not os.path.isdir(site_src):
            continue
        
        site_dst = os.path.join(backup_dir, site_folder)
        
        print(f"\n{Colors.CYAN}Backing up: {site_folder}{Colors.RESET}")
        
        if os.path.exists(site_dst):
            print(f"{Colors.YELLOW}Already exists, skipping{Colors.RESET}")
            skip_count += 1
        else:
            try:
                avg_speed, transfer_time = copy_with_speed(site_src, site_dst)
                print(f"{Colors.GREEN}✓ Backed up successfully{Colors.RESET}")
                print(f"{Colors.GREEN}Transfer time: {format_time(transfer_time)} | Avg speed: {format_speed(avg_speed)}{Colors.RESET}")
                success_count += 1
                logging.info(f"Backed up {site_folder}")
            except Exception as e:
                print(f"{Colors.RED}✗ Error: {e}{Colors.RESET}")
                error_count += 1
                logging.error(f"Failed to backup {site_folder}: {e}")
    
    print(f"\n{Colors.CYAN}Backup Summary: {success_count} copied, {skip_count} skipped, {error_count} errors{Colors.RESET}")
    
    save_config("backup_folder", backup_dir)
    
    total_section_time = time.time() - section_start_time
    print_completion_banner("BACKUP COMPLETE", total_section_time)
    
    # Compare directories
    identical, affected_folders = compare_directories(primary_dir, backup_dir)
    
    # If not identical, display affected folders table
    if not identical and affected_folders:
        print(f"\n\n{Colors.RED}{Colors.BOLD}")
        print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
        print("║                        BACKUP DISCREPANCY REPORT                                       ║")
        print("║                         (Folders Requiring Attention)                                  ║")
        print("╠════════════════════════════════════════════════════════════════════════════════════════╣")
        print(f"{Colors.RESET}")
        
        header = f"{Colors.BOLD}{'Folder':50} | {'Issue':35}{Colors.RESET}"
        print(header)
        print("─" * 90)
        
        # Group by folder and consolidate issues
        folder_issues = {}
        for folder, issue in affected_folders:
            if folder not in folder_issues:
                folder_issues[folder] = []
            if issue not in folder_issues[folder]:
                folder_issues[folder].append(issue)
        
        # Sort folders alphabetically
        for folder in sorted(folder_issues.keys()):
            issues = ", ".join(folder_issues[folder])
            print(f"{Colors.BOLD}{folder[:50]:50}{Colors.RESET} | {Colors.YELLOW}{issues[:35]:35}{Colors.RESET}")
        
        print(f"\n{Colors.RED}{Colors.BOLD}")
        print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
        print(f"{Colors.RESET}")
        print(f"\n{Colors.YELLOW}⚠ Please navigate to the folders listed above and verify the contents manually.{Colors.RESET}\n")
        logging.warning(f"Backup discrepancies found in {len(folder_issues)} folders")
    
    # Verify backup integrity
    verify_data_integrity("backup")

print("DEBUG: Backup operations defined")

# ============================================================================
# MAIN PROGRAM
# ============================================================================

print("DEBUG: Defining main program...")

def main() -> None:
    """Main program execution."""
    try:
        show_title()
        
        # Clear screen before showing intro
        time.sleep(1)
        clear_screen()
        show_intro()
        authenticate()
        
        # Navigation system with BACK functionality
        current_step = 1
        log_dir = None
        parent_folder = None
        
        while current_step <= 7:
            
            if current_step == 1:
                # PART 1: Set Log Directory
                clear_screen()
                print_part_header(1, "SET LOG DIRECTORY")
                
                print(f"""
BEFORE THE TRANSFER OF UAV DATA CAN COMMENSE, A {Colors.CYAN}'LOG DIRECTORY'{Colors.RESET} MUST BE ASSIGNED TO TRACK 
DATA TRANSFERS AND ANY ERRORS THAT MAY OCCURE DURING THE BACKUP PROCESS.
IT IS RECCOMENDED THAT THE LOG DIRECTORY BE ASSIGNED TO THE WORKSTATIONS PHYSICAL DRIVE.
TO SET YOUR LOG DIRECTORY, SIMPLY COPY AND PASTE THE FILE PATH WHERE DIRECTED BELOW.
""")
                
                # Set log directory FIRST, then setup logging
                log_dir = get_log_directory()
                setup_logging()
                
                if proceed_prompt("LOG DIRECTORY SET... READY TO SET PARENT DIRECTORY AND CREATE FOLDER STRUCTURE?"):
                    current_step = 2
                else:
                    print(f"{Colors.YELLOW}Cannot go back from first step. Please restart if needed.{Colors.RESET}")
                    time.sleep(2)
            
            elif current_step == 2:
                # PART 2: Set Parent Directory & Create Folder Structure
                clear_screen()
                print_part_header(2, "SET PARENT DIRECTORY & CREATE FOLDER STRUCTURE")
                
                print(f"""
NOW YOU WILL BE PROMPTED TO ASSIGN A {Colors.CYAN}'PARENT DIRECTORY'{Colors.RESET}.
THE PARENT DIRECTORY IS THE LOCATION WHERE YOUR DATA WILL BE TRANSFERED TO. IN ACCOURDANCE WITH 
THE TERN UAV DATA TRANSFER AND BACKUP PROTOCOL YOUR PARENT DIRECTORY SHOULD BE AN EXTERNAL SSD.
YOUR PARENT DIRECTORY CAN BE SET BY SIMPLY COPYING THE FILE PATH AND PASTING IT WHERE DIRECTED.

AFTER SETTING THE PARENT DIRECTORY, YOU WILL CREATE THE FOLDER STRUCTURE FOR YOUR TERN PLOT IDs.
""")
                
                parent_folder = get_parent_directory()
                create_folder_structure(parent_folder)
                
                print(f"\n{Colors.CYAN}{Colors.BOLD}")
                print("YOU HAVE NOW GENERATED ALL THE RELEVANT FOLDERS TO BEGINE THE DATA TRANSFER")
                print(f"{Colors.RESET}")
                
                if proceed_prompt("READY TO BEGIN DATA TRANSFER?"):
                    current_step = 3
                else:
                    current_step = 1
            
            elif current_step == 3:
                # PART 3: L2 Transfer
                clear_screen()
                transfer_lidar_data()
                
                if proceed_prompt("L2 DATA TRANSFER COMPLETED... READY TO BEGINE TRANSFER OF P1 DATA?"):
                    current_step = 4
                else:
                    current_step = 2
            
            elif current_step == 4:
                # PART 4: P1 Transfer
                clear_screen()
                transfer_p1_rgb_data()
                
                if proceed_prompt("P1 DATA TRANSFER COMPLETED... READY TO BEGIN MICASENSE DATA TRANSFER?"):
                    current_step = 5
                else:
                    current_step = 3
            
            elif current_step == 5:
                # PART 5: MicaSense Transfer
                clear_screen()
                transfer_micasense_data()
                
                if proceed_prompt("MICASENSE DATA TRANSFER COMPLETED... READY TO BEGIN DATA VERIFICATION?"):
                    current_step = 6
                else:
                    current_step = 4
            
            elif current_step == 6:
                # PART 6: Verification
                clear_screen()
                verify_data_integrity("primary")
                
                if proceed_prompt("DATA VERIFICATION COMPLETED... READY TO COMMENCE SSD DATA BACKUP?"):
                    current_step = 7
                else:
                    current_step = 5
            
            elif current_step == 7:
                # PART 7: Backup
                clear_screen()
                ssd_backup()
                
                # Final completion - no BACK option after backup
                break
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}")
        print("╔════════════════════════════════════════════════════════════════════════════════════════╗")
        print("║                           DATA TRANSFER AND BACKUP COMPLETE!                           ║")
        print("╚════════════════════════════════════════════════════════════════════════════════════════╝")
        print(f"{Colors.RESET}")
        
        print(f"\n{Colors.CYAN}Log file saved to: {LOG_FILE}{Colors.RESET}\n")
        logging.info("DroneScape Transfer Protocol completed successfully")
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.RED}⚠ Operation cancelled by user{Colors.RESET}")
        logging.warning("Operation cancelled by user (KeyboardInterrupt)")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}✗ Critical Error: {e}{Colors.RESET}")
        logging.exception(f"Critical error occurred: {e}")
        sys.exit(1)

print("DEBUG: Main function defined successfully")
print("DEBUG: About to check if __name__ == '__main__'")

if __name__ == "__main__":
    print("DEBUG: Running as main script")
    try:
        print("DEBUG: Calling main()...")
        main()
        print("DEBUG: main() completed successfully")
    except Exception as e:
        print(f"\n{Colors.RED}FATAL ERROR: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit...")
        sys.exit(1)

print("DEBUG: Script end reached")