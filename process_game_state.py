import pandas as pd
from shapely.geometry import Point, Polygon

class ProcessGameState:
    """
    Class for reading a parquet file of CS:GO match data.

    Attributes:
        df (pd.DataFrame): The dataframe that holds the data from the parquet file.
    """
    
    def __init__(self, parquet_file_path):
        """
        Constructor for ProcessGameState class.

        Parameters:
            parquet_file_path (str): The path to the parquet file that is read.
        """
        
        self.df = pd.read_parquet(parquet_file_path, engine='pyarrow')

    def get_df(self):
        """
        Get function for df.

        Returns:
            self.df: The dataframe of the parquet file.
        """
        
        return self.df

    def remove_unneeded_cols(self, df, columns):
        """
        Removes unneeded columns within the given df.

        Args:
            df (pd.DataFrame): The df being modified.
            columns (list): The list of columns to keep.

        Returns:
            processed_df: A new dataframe with only the specified columns.
        """
        
        processed_df = df.copy()

        irrelevant_columns = set(processed_df.columns) - set(columns)
        processed_df.drop(columns=irrelevant_columns, inplace=True)

        return processed_df

    def filter_by_team(self, df, team):
        """
        Filters the df based on the team

        Args:
            df (pd.DataFrame): The df being modified.
            team (str): The name of the team.

        Returns:
            filtered_df: A new dataframe with only the team specified.
        """
        
        filtered_df = df[(df['team'] == team)]
        return filtered_df
    
    def filter_by_side(self, df, side):
        """
        Filters the df based on the side

        Args:
            df (pd.DataFrame): The df being modified.
            side (str): The name of the side.

        Returns:
            filtered_df: A new dataframe with only the side specified.
        """
        
        filtered_df = df[(df['side'] == side)]
        return filtered_df

    def filter_by_area_name(self, df, area_names):
        """
        Filters the df based on the area_name

        Args:
            df (pd.DataFrame): The df being modified.
            area_names (list): The list of area_names.

        Returns:
            filtered_df: A new dataframe with only the area_names specified.
        """
        
        filtered_df = df[df['area_name'].isin(area_names)]
        return filtered_df
    
    def filter_by_polygon(self, df, polygon_coordinates, z_bound):
        """
        Filters the df based on given polygon and z bound coordinates.

        Args:
            df (pd.DataFrame): The df being modified.
            polygon_coordinates (list): The list of polygon coordinates.
            z_bound (list): The list of z bound coordinates.

        Returns:
            filtered_df: A new dataframe with only the rows in the given coordinates.
        """
        
        polygon = Polygon(polygon_coordinates)
        filtered_df = df[
            (df['z'].between(z_bound[0], z_bound[1])) &
            df.apply(lambda row: polygon.contains(Point(row['x'], row['y'])), axis=1)
        ]
        return filtered_df
    
    def extract_weapon_classes(self, df):
        """
        Creates a new column from extracted weapon_classes from the inventory column within the given df.
        
        Args:
            df (pd.DataFrame): The df being modified.

        Returns:
            new_df: The df with new column weapon_classes            
        """
        
        new_df = df.copy()
        weapon_classes = []

        for _, row in df.iterrows():
            inventory = row['inventory']
            
            if inventory is None:
                weapon_classes.append([])
            else:
                weapon_classes.append([item['weapon_class'] for item in inventory])   

        new_df['weapon_classes'] = pd.Series(weapon_classes)
        return new_df

    def get_first_row_of_each_round(self, df):
        """
        Gets the first row of each round for the given df.

        Args:
            df (pd.DataFrame): The df being modified.

        Returns:
            new_df: New df made from first_round_rows list           
        """

        first_round_rows = []

        current_round = None

        for _, row in df.iterrows():
            round_number = row['round_num']

            if round_number != current_round:
                first_round_rows.append(row)
                current_round = round_number

        new_df = pd.DataFrame(first_round_rows)
        return new_df
