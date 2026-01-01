import os

# Internal breeder capabilities - not customer configurable
BREEDER_CAPABILITIES = {
    "linux_performance": {
        "supported_target_types": ["ssh"]
    },
    # Future breeders:
    # "api_performance": {
    #     "supported_target_types": ["http", "api"],
    # },
}

class DatabaseConfig:
    ARCHIVE_DB = dict(
        user="yugabyte",
        password="yugabyte", 
        host="yb-tservers.godon.svc.cluster.local",
        port=os.environ.get('YB_TSERVER_SERVICE_SERVICE_PORT_TCP_YSQL_PORT')
    )

    META_DB = dict(
        user="meta_data",
        password="meta_data",
        host=os.environ.get('GODON_METADATA_DB_SERVICE_HOST'),
        port=os.environ.get('GODON_METADATA_DB_SERVICE_PORT')
    )

class BreederConfig:
    @staticmethod
    def extract_breeder_config(request_data):
        if not request_data or 'breeder' not in request_data:
            raise ValueError("Invalid breeder configuration: missing 'breeder' key")

        return request_data.get('breeder', {})

    @staticmethod
    def validate_minimal(breeder_config):
        """Minimal validation to catch catastrophic config errors early
        
        MAKESHIFT IMPLEMENTATION - DO NOT RELY ON THIS FOR PRODUCTION VALIDATION
        
        Why this is makeshift:
        1. Schema is highly experimental and changing rapidly
        2. Adding strict validation now would require constant updates as schema evolves
        3. Better to fail fast on obvious errors than lock in unstable validation rules
        4. Workers will catch detailed schema errors during parameter processing
        
        When to improve this:
        - Once config format stabilizes (no weekly structure changes)
        - When we have real-world config patterns to validate against
        - If we see specific classes of user errors that could be prevented upfront
        
        Current scope: Catch only catastrophic errors (missing required top-level sections)
        Future scope: Full JSON Schema validation once API is stable
        """
        errors = []

        if not breeder_config.get('breeder', {}).get('name'):
            errors.append("Missing breeder.name")

        if not breeder_config.get('objectives') or len(breeder_config.get('objectives', [])) == 0:
            errors.append("Missing or empty objectives array")

        if not breeder_config.get('effectuation', {}).get('targets') or len(breeder_config.get('effectuation', {}).get('targets', [])) == 0:
            errors.append("Missing or empty effectuation.targets array")

        if not breeder_config.get('settings', {}).get('sysctl'):
            errors.append("Missing settings.sysctl configuration")

        # Validate cooperation configuration
        if breeder_config.get('cooperation', {}).get('active', False):
            parallel_workers = breeder_config.get('run', {}).get('parallel', 1)
            if parallel_workers <= 1:
                errors.append(
                    f"Cooperation enabled but run.parallel={parallel_workers}. "
                    "Cooperation requires parallel > 1 for multiple workers to share trials."
                )

        # Validate target type compatibility
        breeder_name = breeder_config.get('breeder', {}).get('name')
        if breeder_name in BREEDER_CAPABILITIES:
            supported_types = BREEDER_CAPABILITIES[breeder_name]['supported_target_types']
            
            for idx, target in enumerate(breeder_config.get('effectuation', {}).get('targets', [])):
                target_type = target.get('type')
                
                if not target_type:
                    errors.append(f"Target {idx}: Missing required 'type' field")
                elif target_type not in supported_types:
                    errors.append(
                        f"Target {idx} ({target.get('address', 'unknown')}): "
                        f"Type '{target_type}' not supported by breeder '{breeder_name}'. "
                        f"Supported types: {supported_types}"
                    )

        if errors:
            error_msg = "Config validation failed:\n" + "\n".join(f"  - {err}" for err in errors)
            raise ValueError(error_msg)

        return True