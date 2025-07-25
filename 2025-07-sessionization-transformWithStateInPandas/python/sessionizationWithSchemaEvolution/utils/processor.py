import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, BooleanType
from typing import Iterator

class SessionizerV1(StatefulProcessor):
    """
    V1 Processor - ENHANCED with debug output to guarantee results
    
    This processor handles user session tracking by:
    - Maintaining session state across streaming events
    - Counting events and accumulating revenue per session
    - Ending sessions when terminal events occur (purchase, logout)
    - Outputting completed session summaries
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize the V1 processor with state schema and configuration.
        
        Args:
            handle: StatefulProcessorHandle for managing state operations
        """
        print("[V1 INIT] ========== INITIALIZING V1 PROCESSOR ==========")
        
        # Define V1 State Schema - basic session tracking fields
        state_schema = StructType([
            StructField("session_id", StringType(), True),      # Unique session identifier
            StructField("user_id", StringType(), True),         # User associated with session
            StructField("event_count", IntegerType(), True),    # Number of events in session
            StructField("total_revenue", DoubleType(), True),   # Cumulative revenue for session
            StructField("session_start", TimestampType(), True) # When the session began
        ])
        
        # Initialize state store with the defined schema
        self.session_state = handle.getValueState("session_state", state_schema)
        
        # Define events that trigger session completion
        self.terminal_events = {'purchase', 'logout'}
        
        print(f"[V1 INIT] Terminal events: {self.terminal_events}")
        print("[V1 INIT] ========== V1 PROCESSOR READY ==========")

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        """
        Process incoming streaming data for a specific session key.
        
        Args:
            key: Session identifier (usually session_id)
            rows: Iterator of pandas DataFrames containing event data
            timer_values: Timer information (not used in this implementation)
            
        Yields:
            pd.DataFrame: Completed session summaries when terminal events occur
        """
        # Extract session ID from key (handle both tuple and string formats)
        session_id = key[0] if isinstance(key, tuple) else str(key)
        print(f"\n[V1 BATCH] ========== PROCESSING BATCH FOR {session_id} ==========")
        
        # Process each batch of events for this session
        for batch in rows:
            # Skip empty batches
            if batch.empty:
                print(f"[V1 BATCH] Empty batch for {session_id}")
                continue
                
            print(f"[V1 BATCH] Batch size: {len(batch)} events for {session_id}")
            results = []  # Store completed sessions for output
            
            # Process each event in the batch
            for idx, event in batch.iterrows():
                try:
                    # Extract event data with type conversion
                    event_type = str(event['event_type'])
                    user_id = str(event['user_id'])
                    revenue = float(event['revenue'])
                    timestamp = pd.Timestamp(event['event_timestamp'])
                    
                    print(f"[V1 EVENT] Processing {session_id}: {event_type} (revenue: {revenue})")
                    
                    # Retrieve current session state from state store
                    current_state = self.session_state.get()
                    
                    if current_state is None:
                        # Create new session state for first event
                        state = (session_id, user_id, 1, revenue, timestamp)
                        print(f"[V1 NEW] Created session {session_id} with first event {event_type}")
                    else:
                        # Update existing session state by incrementing counts
                        state = (
                            current_state[0],              # session_id (unchanged)
                            current_state[1],              # user_id (unchanged)
                            current_state[2] + 1,          # event_count + 1
                            current_state[3] + revenue,    # total_revenue + current revenue
                            current_state[4]               # session_start (unchanged)
                        )
                        print(f"[V1 UPDATE] Updated {session_id}: count={state[2]}, revenue={state[3]}")
                    
                    # Check if this event should terminate the session
                    is_terminal = event_type in self.terminal_events
                    print(f"[V1 CHECK] Event {event_type} terminal? {is_terminal}")
                    
                    if is_terminal:
                        # Session completed - create output record
                        result = {
                            'session_id': state[0],
                            'user_id': state[1], 
                            'event_count': int(state[2]),
                            'total_revenue': float(state[3]),
                            'session_start': state[4],
                            'schema_version': 'v1'  # Mark as V1 output
                        }
                        results.append(result)
                        print(f"[V1 COMPLETE] SESSION COMPLETED: {session_id} with {state[2]} events, revenue: {state[3]}")
                        
                        # Clear state since session is complete
                        self.session_state.clear()
                        print(f"[V1 COMPLETE] State cleared for {session_id}")
                    else:
                        # Session continues - update state store with new values
                        self.session_state.update(state)
                        print(f"[V1 CONTINUE] State updated for {session_id}, waiting for terminal event")
                        
                except Exception as e:
                    # Log any processing errors and continue
                    print(f"[V1 ERROR] Error processing event: {e}")
                    import traceback
                    traceback.print_exc()
            
            print(f"[V1 BATCH] Batch complete. Results: {len(results)} sessions completed")
            
            # Output completed sessions or empty DataFrame with correct schema
            if results:
                result_df = pd.DataFrame(results)
                print(f"[V1 OUTPUT] OUTPUTTING {len(results)} COMPLETED SESSIONS:")
                for i, row in result_df.iterrows():
                    print(f"[V1 OUTPUT]   {row['session_id']}: {row['event_count']} events, ${row['total_revenue']}")
                yield result_df
            else:
                # Return empty DataFrame with correct schema for downstream compatibility
                empty_df = pd.DataFrame(columns=['session_id', 'user_id', 'event_count', 'total_revenue', 'session_start', 'schema_version'])
                print(f"[V1 OUTPUT] No completed sessions in this batch")
                yield empty_df

    def close(self) -> None:
        """
        Clean up resources when processor is shut down.
        """
        print("[V1 CLOSE] ========== V1 PROCESSOR CLOSED ==========")
        pass


class SessionizerV2(StatefulProcessor):
    """
    FIXED V2 Processor - Let Databricks handle schema evolution automatically
    
    This is an evolved version of the V1 processor that:
    - Adds new fields (device_type, page_category)
    - Widens event_count from Integer to Long
    - Automatically handles schema evolution from V1 state
    - Maintains backward compatibility with V1 sessions
    """
    
    def init(self, handle: StatefulProcessorHandle) -> None:
        """
        Initialize the V2 processor with evolved state schema.
        
        Args:
            handle: StatefulProcessorHandle for managing state operations
        """
        print("[V2 INIT] ========== INITIALIZING FIXED V2 ==========")
        
        # V2 State Schema - evolved from V1 with additional fields and type widening
        state_schema = StructType([
            StructField("session_id", StringType(), True),      # Same as V1
            StructField("user_id", StringType(), True),         # Same as V1
            StructField("event_count", LongType(), True),       # TYPE WIDENING: Int->Long for larger counts
            StructField("total_revenue", DoubleType(), True),   # Same as V1
            StructField("session_start", TimestampType(), True), # Same as V1
            StructField("device_type", StringType(), True),     # NEW FIELD: Track user's device
            StructField("page_category", StringType(), True)    # NEW FIELD: Track page category
        ])
        
        # Initialize state store - Databricks automatically handles schema evolution
        self.session_state = handle.getValueState("session_state", state_schema)
        
        # Same terminal events as V1
        self.terminal_events = {'purchase', 'logout'}
        
        print("[V2 INIT] V2 processor ready - Databricks will handle schema evolution automatically")

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timer_values) -> Iterator[pd.DataFrame]:
        """
        Process incoming streaming data with schema evolution support.
        
        This method can handle both:
        - New V2 sessions with all fields
        - Existing V1 sessions that need to be evolved to V2 format
        
        Args:
            key: Session identifier
            rows: Iterator of pandas DataFrames containing event data
            timer_values: Timer information (not used)
            
        Yields:
            pd.DataFrame: Completed session summaries with V2 schema
        """
        session_id = key[0] if isinstance(key, tuple) else str(key)
        print(f"[V2] ========== PROCESSING {session_id} ==========")
        
        for batch in rows:
            if batch.empty:
                continue
                
            results = []  # Store completed sessions
            
            # Process each event in the batch
            for _, event in batch.iterrows():
                try:
                    # Extract event data with safe defaults for new V2 fields
                    event_type = str(event['event_type'])
                    user_id = str(event['user_id'])
                    revenue = float(event['revenue'])
                    timestamp = pd.Timestamp(event['event_timestamp'])
                    
                    # Handle new V2 fields with fallback to 'unknown'
                    device_type = str(event.get('device_type', 'unknown')) if event.get('device_type') is not None else 'unknown'
                    page_category = str(event.get('page_category', 'unknown')) if event.get('page_category') is not None else 'unknown'
                    
                    print(f"[V2] Event: {event_type}, Device: {device_type}, Session: {session_id}")
                    
                    # Get current state - Databricks automatically handles schema evolution
                    current_state = self.session_state.get()
                    evolved_from_v1 = False  # Track if this session was evolved from V1
                    
                    if current_state is None:
                        print(f"[V2] NEW SESSION: {session_id}")
                        # Create new V2 session with all fields
                        state = (session_id, user_id, 1, revenue, timestamp, device_type, page_category)
                    else:
                        print(f"[V2] EXISTING STATE: {session_id}")
                        
                        # Detect schema evolution: V1 state will have None values in new fields
                        # When V1 state is read by V2 processor, new fields (indices 5,6) will be None
                        if current_state[5] is None or current_state[6] is None:
                            print(f"[V2] 🎉 EVOLUTION DETECTED: {session_id} has V1 state with None values in new fields!")
                            evolved_from_v1 = True
                            
                            # Evolve V1 state to V2 format using current event's data
                            state = (
                                current_state[0],              # session_id (unchanged)
                                current_state[1],              # user_id (unchanged)
                                current_state[2] + 1,          # event_count (auto-converted Int->Long) + 1
                                current_state[3] + revenue,    # total_revenue + current revenue
                                current_state[4],              # session_start (unchanged)
                                device_type,                   # device_type (was None, now set from event)
                                page_category                  # page_category (was None, now set from event)
                            )
                            print(f"[V2] EVOLVED: {session_id} from V1 -> V2 with {state[2]} events")
                        else:
                            print(f"[V2] REGULAR UPDATE: {session_id} already has V2 format")
                            # Already V2 format - perform regular update
                            state = (
                                current_state[0],              # session_id (unchanged)
                                current_state[1],              # user_id (unchanged)
                                current_state[2] + 1,          # event_count + 1
                                current_state[3] + revenue,    # total_revenue + revenue
                                current_state[4],              # session_start (unchanged)
                                current_state[5],              # device_type (keep existing)
                                current_state[6]               # page_category (keep existing)
                            )
                    
                    # Check if this event terminates the session
                    is_terminal = event_type in self.terminal_events
                    print(f"[V2] Terminal check: {event_type} -> {is_terminal}")
                    
                    if is_terminal:
                        # Session completed - create V2 output record
                        result = {
                            'session_id': state[0],
                            'user_id': state[1],
                            'event_count': state[2],           # Now Long type
                            'total_revenue': float(state[3]),
                            'session_start': state[4],
                            'device_type': state[5],           # New V2 field
                            'page_category': state[6],         # New V2 field
                            'schema_version': 'v2',            # Mark as V2 output
                            'evolved_from_v1': evolved_from_v1 # Track evolution history
                        }
                        results.append(result)
                        
                        # Log completion with evolution status
                        evolution_msg = "EVOLVED FROM V1" if evolved_from_v1 else "NEW V2 SESSION"
                        print(f"[V2] COMPLETED: {session_id} - {evolution_msg}")
                        print(f"[V2] OUTPUT: {state[2]} events, ${state[3]}, {state[5]} device")
                        
                        # Clear state since session is complete
                        self.session_state.clear()
                    else:
                        # Session continues - update state store
                        self.session_state.update(state)
                        print(f"[V2] CONTINUE: {session_id} waiting for terminal event")
                        
                except Exception as e:
                    # Log processing errors and continue
                    print(f"[V2] ERROR: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Output results with V2 schema
            if results:
                print(f"[V2] BATCH OUTPUT: {len(results)} completed sessions")
                yield pd.DataFrame(results)
            else:
                # Return empty DataFrame with complete V2 schema
                print(f"[V2] No completed sessions this batch")
                empty_columns = ['session_id', 'user_id', 'event_count', 'total_revenue', 
                               'session_start', 'device_type', 'page_category', 'schema_version', 'evolved_from_v1']
                yield pd.DataFrame(columns=empty_columns)

    def close(self) -> None:
        """
        Clean up resources when processor is shut down.
        """
        print("[V2] V2 processor closed")
        pass