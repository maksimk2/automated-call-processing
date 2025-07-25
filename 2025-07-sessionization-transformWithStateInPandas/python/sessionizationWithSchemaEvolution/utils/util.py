# =============================================================================
# Utility Functions for PySpark Streaming Sessionization Demo
# =============================================================================
# This module provides utility functions for:
# 1. Project directory management in Databricks environment
# 2. Synthetic streaming data generation for schema evolution testing
# 3. Ensuring proper V1/V2 state persistence and evolution scenarios
# =============================================================================

def get_project_dir():
    """
    Get project directory path for the sessionization demo.
    
    This function constructs a user-specific project directory path within
    Databricks volumes, ensuring each user has their own isolated workspace
    for the demo data and checkpoints.
    
    Returns:
        str: Full path to the user's project directory in the format:
             /Volumes/main/demos/demos_volume/{username}/python/transformwithstate/sessionization
    
    Note:
        - Uses Databricks DBUtils to get the current user's username
        - Creates a user-specific directory to avoid conflicts between users
        - Uses Databricks Volumes for persistent storage across cluster restarts
    """
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    
    # Get current Spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Initialize Databricks utilities for accessing workspace context
    dbutils = DBUtils(spark)
    
    # Extract the current user's username from the notebook context
    # This ensures each user gets their own isolated directory
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    
    # Define base volume path for persistent storage
    volume_path = "/Volumes/main/demos/demos_volume"
    
    # Construct user-specific project directory path
    # Format: /Volumes/.../username/python/transformwithstate/sessionization
    projectDir = f"{volume_path}/{username}/python/transformwithstate/sessionization"
    
    return projectDir

def create_demo_data(spark):
    """
    Create synthetic streaming data designed for schema evolution testing.
    
    This function generates a rate stream with carefully crafted data patterns
    that demonstrate PySpark streaming sessionization and schema evolution:
    
    Key Features:
    - Creates overlapping session IDs across V1 and V2 events
    - Ensures some V1 sessions remain active for V2 processor evolution
    - Generates both terminal and non-terminal events in specific patterns
    - Adds V2-specific fields (device_type, page_category) only for V2 events
    
    Args:
        spark: SparkSession object for creating streaming DataFrames
        
    Returns:
        DataFrame: Streaming DataFrame with the following schema:
            - session_id: String (session_1 to session_6, cycling)
            - user_id: String (user_1 to user_6, matching session IDs)
            - event_timestamp: Timestamp (from rate stream)
            - event_type: String (page_view, purchase, logout, search)
            - revenue: Double (25.0, 50.0, 35.0, or 0.0)
            - schema_version: String ("v1" for events 0-11, "v2" for 12+)
            - device_type: String (mobile, desktop, tablet, smartphone) - V2 only
            - page_category: String (checkout, profile, product, home) - V2 only
    
    Evolution Strategy:
    - V1 Events (0-11): Some sessions get terminal events, others remain active
    - V2 Events (12+): Previously active V1 sessions get terminal events
    - This ensures V2 processor can demonstrate evolution of existing V1 state
    """
    from pyspark.sql.functions import when, lit, col
    
    # Create base rate stream - generates sequential events with timestamps
    # rowsPerSecond=1 provides steady stream for demo purposes
    rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    
    # Transform rate stream into sessionization demo data
    # The 'value' column from rate stream (0, 1, 2, ...) drives all logic
    demo_stream = rate_stream.select(
        
        # =============================================================================
        # Session ID Generation - Cycling Pattern
        # =============================================================================
        # Create 6 different sessions that cycle based on value % 6
        # This ensures predictable session distribution across events
        when(col("value") % 6 == 0, "session_1")
        .when(col("value") % 6 == 1, "session_2") 
        .when(col("value") % 6 == 2, "session_3")
        .when(col("value") % 6 == 3, "session_4")
        .when(col("value") % 6 == 4, "session_5")
        .otherwise("session_6").alias("session_id"),
        
        # =============================================================================
        # User ID Generation - Matches Session IDs
        # =============================================================================
        # Each session belongs to a specific user (1:1 mapping)
        # This simplifies the sessionization logic and makes results predictable
        when(col("value") % 6 == 0, "user_1")
        .when(col("value") % 6 == 1, "user_2")
        .when(col("value") % 6 == 2, "user_3") 
        .when(col("value") % 6 == 3, "user_4")
        .when(col("value") % 6 == 4, "user_5")
        .otherwise("user_6").alias("user_id"),
        
        # =============================================================================
        # Timestamp - Direct from Rate Stream
        # =============================================================================
        # Use the rate stream's timestamp for event ordering
        col("timestamp").alias("event_timestamp"),
        
        # =============================================================================
        # Event Type Generation - CRITICAL FOR SCHEMA EVOLUTION
        # =============================================================================
        # This is the most important part for demonstrating schema evolution:
        # 
        # V1 Phase (events 0-11):
        # - session_2 and session_4 get TERMINAL events (purchase/logout)
        # - session_1, session_3, session_5, session_6 get NON-terminal events
        # - This leaves 4 sessions with ACTIVE state in the state store
        #
        # V2 Phase (events 12+):
        # - Previously active sessions (1,3,5,6) now get terminal events
        # - This demonstrates V2 processor reading and evolving V1 state
        when(col("value") < 12,  # V1 events (first 12 events: 0-11)
             when(col("value") % 6 == 1, "purchase")     # session_2 terminates in V1
             .when(col("value") % 6 == 3, "logout")      # session_4 terminates in V1
             .otherwise("page_view")                     # sessions 1,3,5,6 remain active
        ).otherwise(  # V2 events (events 12 and beyond)
             when(col("value") % 6 == 0, "purchase")     # session_1 terminates in V2 (evolution!)
             .when(col("value") % 6 == 2, "logout")      # session_3 terminates in V2 (evolution!)
             .when(col("value") % 6 == 4, "purchase")    # session_5 terminates in V2 (evolution!)
             .otherwise("search")                        # session_6 and new cycles continue
        ).alias("event_type"),
        
        # =============================================================================
        # Revenue Generation - Matches Purchase Events
        # =============================================================================
        # Add revenue only for purchase events to make results more realistic
        # Different sessions generate different revenue amounts
        when(col("value") % 6 == 1, 25.0)  # session_2 purchase in V1
        .when(col("value") % 6 == 0, 50.0) # session_1 purchase in V2 (evolution)
        .when(col("value") % 6 == 4, 35.0) # session_5 purchase in V2 (evolution)
        .otherwise(0.0).alias("revenue"),   # All other events have 0 revenue
        
        # =============================================================================
        # Schema Version - Temporal Separation
        # =============================================================================
        # First 12 events (0-11) are V1 schema events
        # Events 12+ are V2 schema events with additional fields
        # This temporal separation simulates a real schema migration scenario
        when(col("value") < 12, "v1").otherwise("v2").alias("schema_version"),
        
        # =============================================================================
        # Device Type - V2 Schema Addition
        # =============================================================================
        # This field only exists for V2 events (value >= 12)
        # V1 events will have NULL/None for this field
        # This demonstrates schema evolution with new field addition
        when(col("value") >= 12,
             when(col("value") % 6 == 0, "mobile")       # session_1 uses mobile
             .when(col("value") % 6 == 2, "desktop")     # session_3 uses desktop
             .when(col("value") % 6 == 4, "tablet")      # session_5 uses tablet
             .otherwise("smartphone")                    # other sessions use smartphone
        ).alias("device_type"),  # NULL for V1 events
        
        # =============================================================================
        # Page Category - V2 Schema Addition
        # =============================================================================
        # Another V2-only field that demonstrates schema evolution
        # V1 events will have NULL/None for this field
        # Maps to different page types for business context
        when(col("value") >= 12,
             when(col("value") % 6 == 0, "checkout")     # session_1 on checkout page
             .when(col("value") % 6 == 2, "profile")     # session_3 on profile page
             .when(col("value") % 6 == 4, "product")     # session_5 on product page
             .otherwise("home")                          # other sessions on home page
        ).alias("page_category")  # NULL for V1 events
    )
    
    return demo_stream

# =============================================================================
# Data Generation Summary
# =============================================================================
# 
# Expected Data Flow:
# 
# V1 Events (0-11):
# - Events 0,6: session_1/user_1, page_view, revenue=0, v1 schema
# - Events 1,7: session_2/user_2, purchase, revenue=25, v1 schema (TERMINATES)
# - Events 2,8: session_3/user_3, page_view, revenue=0, v1 schema  
# - Events 3,9: session_4/user_4, logout, revenue=0, v1 schema (TERMINATES)
# - Events 4,10: session_5/user_5, page_view, revenue=0, v1 schema
# - Events 5,11: session_6/user_6, page_view, revenue=0, v1 schema
# 
# After V1: Sessions 1,3,5,6 have ACTIVE state in state store
# 
# V2 Events (12+):
# - Event 12: session_1, purchase, revenue=50, mobile, checkout (EVOLVES + TERMINATES)
# - Event 14: session_3, logout, revenue=0, desktop, profile (EVOLVES + TERMINATES) 
# - Event 16: session_5, purchase, revenue=35, tablet, product (EVOLVES + TERMINATES)
# - Other events continue the pattern...
# 
# This design guarantees that V2 processor will encounter existing V1 state
# and demonstrate successful schema evolution in a controlled, predictable way.
# =============================================================================