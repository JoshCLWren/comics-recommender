import os
import pandas as pd
import gspread
import time
import logging
from math import log
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('comics-recommender')

# Your existing algorithm functions
def calculate_aggregate_score(row, weights):
    """Calculate the aggregate recommendation score for a comic series."""
    next_issue = row['Next Issue']
    aggregate = 0
    contributions = {}
    
    if next_issue > 1:  # Started series
        started_column_map = {
            'Completion Weight': 'Weighted Completion',
            'Rating Weight': 'Last Issues Rating',
            'Efficiency Weight': 'Reading Efficiency',
            'Momentum Weight': 'Momentum-Based',
            'Gap Weight': 'Issue Gap Minimizer',
        }
        for label, col in started_column_map.items():
            weight = weights.get(label, 0)
            try:
                value = pd.to_numeric(row.get(col, 0), errors='coerce')
                value = 0 if pd.isna(value) else value
                contribution = value * weight
                contributions[label] = contribution
                aggregate += contribution
            except:
                contributions[label] = 0
                
    else:  # Unstarted series
        # Rating component
        rating_weight = weights.get('Unstarted Rating Weight', 0)
        try:
            rating_value = row.get('Last Issues Rating')
            if pd.isna(rating_value) or rating_value == '' or rating_value == 0:
                rating = 2.5
            else:
                rating = pd.to_numeric(rating_value, errors='coerce')
                rating = 2.5 if pd.isna(rating) else rating
            
            contribution = rating * rating_weight
            contributions['Unstarted Rating'] = contribution
            aggregate += contribution
        except:
            contributions['Unstarted Rating'] = 2.5 * rating_weight
            aggregate += 2.5 * rating_weight
        
        # Gap component
        gap_weight = weights.get('Unstarted Gap Weight', 0)
        try:
            total_issues = pd.to_numeric(row.get('Total issues', 1), errors='coerce')
            gap_score = min(5.0, 1.0 + log(max(1, total_issues)) / log(2))
            contribution = gap_score * gap_weight
            contributions['Unstarted Gap'] = contribution
            aggregate += contribution
        except:
            contributions['Unstarted Gap'] = 2.0 * gap_weight
            aggregate += 2.0 * gap_weight
        
        # Efficiency score
        efficiency_weight = weights.get('Unstarted Efficiency Weight', 0)
        try:
            total_issues = pd.to_numeric(row.get('Total issues', 1), errors='coerce')
            
            if total_issues <= 3:
                efficiency_score = 4.0
            elif total_issues <= 10:
                efficiency_score = 3.5
            elif total_issues <= 25:
                efficiency_score = 3.0
            elif total_issues <= 50:
                efficiency_score = 2.5
            else:
                efficiency_score = 2.0
                
            contribution = efficiency_score * efficiency_weight
            contributions['Unstarted Efficiency'] = contribution
            aggregate += contribution
        except:
            contributions['Unstarted Efficiency'] = 3.0 * efficiency_weight
            aggregate += 3.0 * efficiency_weight
    
    return aggregate, contributions

def recommend_next_comic(sidequests_df, weights_df):
    """Generate recommendations based on weighted scoring."""
    # Convert weight labels to a dictionary for easier lookup
    weights = weights_df.set_index('Label')['Value'].to_dict()
    
    # Apply the aggregate score calculation to each row
    results = sidequests_df.apply(
        lambda row: calculate_aggregate_score(row, weights), axis=1)
    
    # Extract the scores and contributions
    sidequests_df['Aggregate'] = [result[0] for result in results]
    sidequests_df['Score Breakdown'] = [result[1] for result in results]
    
    # Sort the dataframe by aggregate score in descending order
    return sidequests_df.sort_values('Aggregate', ascending=False)

# Google Sheets integration
def setup_sheets_client():
    """Connect to Google Sheets API using service account credentials."""
    # Use service account file from environment or file
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = Credentials.from_service_account_file(
        credentials_path, scopes=scopes
    )
    
    return gspread.authorize(credentials)

def check_for_updates(gc, spreadsheet_id, last_modified=None):
    """Check if the spreadsheet has been modified since last check."""
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        drive_file = gc.drive.files().get(
            fileId=spreadsheet_id, 
            fields='modifiedTime'
        ).execute()
        
        current_modified = drive_file.get('modifiedTime')
        
        if last_modified is None or current_modified > last_modified:
            return current_modified, True
        
        return last_modified, False
    except Exception as e:
        logger.error(f"Error checking for updates: {e}")
        # Return no update needed if there's an error
        return last_modified, False

def update_recommendations(gc, spreadsheet_id):
    """Process spreadsheet data and update with new recommendations."""
    try:
        # Open spreadsheet and worksheets
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        # Get data from specific sheets
        comics_sheet = spreadsheet.worksheet("Comics")
        weights_sheet = spreadsheet.worksheet("Weights")
        
        # Create or get results sheet
        try:
            results_sheet = spreadsheet.worksheet("Recommendations")
        except gspread.exceptions.WorksheetNotFound:
            results_sheet = spreadsheet.add_worksheet(
                title="Recommendations", rows=100, cols=20
            )
        
        # Get data as pandas DataFrames
        comics_data = comics_sheet.get_all_records()
        weights_data = weights_sheet.get_all_records()
        
        sidequests_df = pd.DataFrame(comics_data)
        weights_df = pd.DataFrame(weights_data)
        
        # Run recommendation algorithm
        recommendations = recommend_next_comic(sidequests_df, weights_df)
        
        # Format Score Breakdown for Google Sheets
        recommendations['Score Breakdown'] = recommendations['Score Breakdown'].apply(str)
        
        # Update results sheet
        headers = recommendations.columns.tolist()
        results_sheet.update('A1', [headers])
        
        # Then update data rows
        if len(recommendations) > 0:
            data_values = recommendations.values.tolist()
            results_sheet.update('A2', data_values)
        
        logger.info(f"Recommendations updated successfully")
        return True
    except Exception as e:
        logger.error(f"Error updating recommendations: {e}")
        return False

def run_service():
    """Main service loop checking for updates and processing recommendations."""
    # Get configuration from environment
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    check_interval = int(os.getenv('CHECK_INTERVAL', '60'))
    
    if not spreadsheet_id:
        logger.error("SPREADSHEET_ID environment variable is required")
        return
    
    logger.info(f"Starting comics recommendation service")
    logger.info(f"Monitoring spreadsheet ID: {spreadsheet_id}")
    logger.info(f"Update check interval: {check_interval} seconds")
    
    gc = setup_sheets_client()
    last_modified = None
    
    while True:
        try:
            last_modified, should_update = check_for_updates(
                gc, spreadsheet_id, last_modified
            )
            
            if should_update:
                logger.info("Changes detected, updating recommendations...")
                success = update_recommendations(gc, spreadsheet_id)
                if success:
                    logger.info("Recommendations updated successfully")
                else:
                    logger.error("Failed to update recommendations")
            else:
                logger.debug("No changes detected")
            
            time.sleep(check_interval)
        except Exception as e:
            logger.error(f"Error in service loop: {e}")
            time.sleep(check_interval)

if __name__ == "__main__":
    run_service()
