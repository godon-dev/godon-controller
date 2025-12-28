import os

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

        if errors:
            error_msg = "Config validation failed:\n" + "\n".join(f"  - {err}" for err in errors)
            raise ValueError(error_msg)

        return True