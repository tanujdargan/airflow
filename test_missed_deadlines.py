#!/usr/bin/env python3
"""
Simple test script to verify the missed deadlines filter implementation.
"""
import sys
import os
from datetime import datetime, timedelta

# Add the airflow-core src to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'airflow-core', 'src'))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Import the models and filter
from airflow.models.base import Base
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.api_fastapi.common.parameters import _MissedDeadlinesFilter

def test_missed_deadlines_filter():
    """Test the missed deadlines filter logic."""
    
    # Create an in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Create test data
    now = datetime.utcnow()
    
    # Test case 1: DagRun with deadline missed (deadline_time < last_scheduling_decision)
    dagrun1 = DagRun(
        dag_id="test_dag_1",
        run_id="test_run_1",
        logical_date=now - timedelta(hours=3),
        last_scheduling_decision=now - timedelta(hours=1),  # Recent decision
        state="running"
    )
    
    deadline1 = Deadline(
        deadline_time=now - timedelta(hours=2),  # Deadline was 2 hours ago
        dag_id="test_dag_1",
        dagrun_id=None,  # Will be set after dagrun1 is added
        callback="test_callback",
    )
    
    # Test case 2: DagRun with deadline NOT missed (deadline_time > last_scheduling_decision)
    dagrun2 = DagRun(
        dag_id="test_dag_2", 
        run_id="test_run_2",
        logical_date=now - timedelta(hours=3),
        last_scheduling_decision=now - timedelta(hours=2),  # Decision was 2 hours ago
        state="running"
    )
    
    deadline2 = Deadline(
        deadline_time=now - timedelta(hours=1),  # Deadline was 1 hour ago (after decision)
        dag_id="test_dag_2",
        dagrun_id=None,  # Will be set after dagrun2 is added
        callback="test_callback",
    )
    
    # Test case 3: DagRun without last_scheduling_decision
    dagrun3 = DagRun(
        dag_id="test_dag_3",
        run_id="test_run_3", 
        logical_date=now - timedelta(hours=3),
        last_scheduling_decision=None,  # No scheduling decision yet
        state="queued"
    )
    
    deadline3 = Deadline(
        deadline_time=now - timedelta(hours=1),
        dag_id="test_dag_3",
        dagrun_id=None,
        callback="test_callback",
    )
    
    # Add to session
    session.add(dagrun1)
    session.add(dagrun2) 
    session.add(dagrun3)
    session.flush()  # Get IDs
    
    # Set the dagrun_ids
    deadline1.dagrun_id = dagrun1.id
    deadline2.dagrun_id = dagrun2.id  
    deadline3.dagrun_id = dagrun3.id
    
    session.add(deadline1)
    session.add(deadline2)
    session.add(deadline3)
    session.commit()
    
    # Test the filter
    base_query = select(DagRun)
    
    # Test with missed_deadlines=True
    missed_filter = _MissedDeadlinesFilter(value=True)
    filtered_query = missed_filter.to_orm(base_query)
    
    results = session.execute(filtered_query).scalars().all()
    
    print(f"Found {len(results)} DagRuns with missed deadlines")
    for result in results:
        print(f"  - {result.dag_id}: {result.run_id}")
    
    # Verify the results
    assert len(results) == 1, f"Expected 1 DagRun with missed deadline, got {len(results)}"
    assert results[0].dag_id == "test_dag_1", f"Expected test_dag_1, got {results[0].dag_id}"
    
    print("✅ Test passed: missed deadlines filter works correctly!")
    
    session.close()

if __name__ == "__main__":
    test_missed_deadlines_filter()
