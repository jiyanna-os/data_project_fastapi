"""
Import status tracking utility for long-running ODS file imports
"""
import json
import time
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

class ImportStatusTracker:
    """Track the status of long-running import operations"""
    
    def __init__(self):
        self.status_file = Path("import_status.json")
        self.current_import = None
    
    def start_import(self, filename: str, file_size_mb: float) -> str:
        """Start tracking a new import operation"""
        import_id = f"import_{int(time.time())}"
        
        # Detect file type for better duration estimation
        file_extension = Path(filename).suffix.lower()
        file_type = "ODS" if file_extension == '.ods' else "XLSX" if file_extension in ['.xlsx', '.xls'] else "UNKNOWN"
        
        status = {
            "import_id": import_id,
            "filename": filename,
            "file_size_mb": file_size_mb,
            "file_type": file_type,
            "status": "starting",
            "phase": "initialization", 
            "progress": 0,
            "start_time": datetime.now().isoformat(),
            "current_step": "Starting import process",
            "estimated_duration_minutes": self._estimate_duration(file_size_mb, file_type),
            "phases": {
                "parquet_conversion": {"status": "pending", "start_time": None, "duration": None},
                "data_import": {"status": "pending", "start_time": None, "duration": None}
            }
        }
        
        self._save_status(status)
        self.current_import = import_id
        return import_id
    
    def update_phase(self, phase: str, step: str, progress: int = None):
        """Update the current phase and step"""
        if not self.current_import:
            return
            
        status = self._load_status()
        if not status:
            return
            
        status["phase"] = phase
        status["current_step"] = step
        status["last_updated"] = datetime.now().isoformat()
        
        if progress is not None:
            status["progress"] = progress
            
        # Update phase-specific info
        if phase in status["phases"]:
            if status["phases"][phase]["status"] == "pending":
                status["phases"][phase]["status"] = "in_progress"
                status["phases"][phase]["start_time"] = datetime.now().isoformat()
        
        self._save_status(status)
    
    def complete_phase(self, phase: str):
        """Mark a phase as completed"""
        if not self.current_import:
            return
            
        status = self._load_status()
        if not status:
            return
            
        if phase in status["phases"]:
            phase_info = status["phases"][phase]
            phase_info["status"] = "completed"
            
            if phase_info["start_time"]:
                start_time = datetime.fromisoformat(phase_info["start_time"])
                duration = (datetime.now() - start_time).total_seconds()
                phase_info["duration"] = duration
        
        self._save_status(status)
    
    def complete_import(self, stats: Dict):
        """Mark the import as completed"""
        if not self.current_import:
            return
            
        status = self._load_status()
        if not status:
            return
            
        status["status"] = "completed"
        status["progress"] = 100
        status["end_time"] = datetime.now().isoformat()
        status["current_step"] = "Import completed successfully"
        status["stats"] = stats
        
        # Calculate total duration
        if "start_time" in status:
            start_time = datetime.fromisoformat(status["start_time"])
            total_duration = (datetime.now() - start_time).total_seconds()
            status["total_duration_seconds"] = total_duration
        
        self._save_status(status)
        self.current_import = None
    
    def fail_import(self, error_message: str):
        """Mark the import as failed"""
        if not self.current_import:
            return
            
        status = self._load_status()
        if not status:
            return
            
        status["status"] = "failed"
        status["error"] = error_message
        status["end_time"] = datetime.now().isoformat()
        status["current_step"] = f"Import failed: {error_message}"
        
        self._save_status(status)
        self.current_import = None
    
    def get_status(self, import_id: str = None) -> Optional[Dict]:
        """Get the current import status"""
        if import_id:
            # Try to load specific import status
            specific_file = Path(f"import_status_{import_id}.json")
            if specific_file.exists():
                with open(specific_file, 'r') as f:
                    return json.load(f)
        
        return self._load_status()
    
    def _estimate_duration(self, file_size_mb: float, file_type: str = "ODS") -> int:
        """Estimate import duration based on file size and type"""
        # XLSX files are generally faster than ODS files
        multiplier = 0.6 if file_type == "XLSX" else 1.0
        
        if file_size_mb > 25:
            base_time = 40  # 40 minutes for large ODS files
        elif file_size_mb > 15:
            base_time = 20  # 20 minutes for medium ODS files
        elif file_size_mb > 5:
            base_time = 10  # 10 minutes for small-medium ODS files
        else:
            base_time = 5   # 5 minutes for small ODS files
        
        # Apply file type multiplier
        estimated_time = int(base_time * multiplier)
        return max(estimated_time, 3)  # Minimum 3 minutes
    
    def _load_status(self) -> Optional[Dict]:
        """Load status from file"""
        if not self.status_file.exists():
            return None
            
        try:
            with open(self.status_file, 'r') as f:
                return json.load(f)
        except Exception:
            return None
    
    def _save_status(self, status: Dict):
        """Save status to file"""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=2)
        except Exception:
            pass  # Fail silently - status tracking is not critical

# Global instance
import_tracker = ImportStatusTracker()