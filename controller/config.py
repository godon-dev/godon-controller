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
    def validate_constraints_v03(constraints_list, param_name):
        """Validate constraint structure (list of ranges or values)

        Args:
            constraints_list: List of constraint objects
            param_name: Parameter name (for error messages)

        Returns:
            str: Constraint type ("int" or "categorical")

        Raises:
            ValueError: If constraint structure is invalid
        """
        if not isinstance(constraints_list, list):
            raise ValueError(
                f"Parameter '{param_name}': constraints must be a list, got {type(constraints_list).__name__}"
            )

        if len(constraints_list) == 0:
            raise ValueError(
                f"Parameter '{param_name}': constraints list cannot be empty"
            )

        # Determine constraint type from first constraint
        # All constraints in list should be same type (int ranges OR categorical values)
        first_constraint = constraints_list[0]

        if not isinstance(first_constraint, dict):
            raise ValueError(
                f"Parameter '{param_name}': each constraint must be a dict, got {type(first_constraint).__name__}"
            )

        # Check if categorical (has 'values' key)
        if 'values' in first_constraint:
            # Validate categorical constraint
            values = first_constraint['values']

            if not isinstance(values, list):
                raise ValueError(
                    f"Parameter '{param_name}': 'values' must be a list, got {type(values).__name__}"
                )

            if len(values) < 2:
                raise ValueError(
                    f"Parameter '{param_name}': 'values' must have at least 2 items, got {len(values)}"
                )

            # Verify all values are strings
            for i, value in enumerate(values):
                if not isinstance(value, str):
                    raise ValueError(
                        f"Parameter '{param_name}': all values must be strings, item {i} is {type(value).__name__}"
                    )

            return "categorical"

        # Check if integer range (has 'step', 'lower', 'upper')
        elif 'step' in first_constraint or 'lower' in first_constraint or 'upper' in first_constraint:
            # Validate integer range constraints
            for i, constraint in enumerate(constraints_list):
                if not isinstance(constraint, dict):
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: must be a dict, got {type(constraint).__name__}"
                    )

                # Check required fields for integer ranges
                missing_fields = []
                if 'step' not in constraint:
                    missing_fields.append('step')
                if 'lower' not in constraint:
                    missing_fields.append('lower')
                if 'upper' not in constraint:
                    missing_fields.append('upper')

                if missing_fields:
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: missing required fields: {', '.join(missing_fields)}"
                    )

                # Validate field types
                if not isinstance(constraint['step'], (int, float)):
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: 'step' must be numeric, got {type(constraint['step']).__name__}"
                    )

                if not isinstance(constraint['lower'], (int, float)):
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: 'lower' must be numeric, got {type(constraint['lower']).__name__}"
                    )

                if not isinstance(constraint['upper'], (int, float)):
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: 'upper' must be numeric, got {type(constraint['upper']).__name__}"
                    )

                # Validate range logic
                if constraint['lower'] >= constraint['upper']:
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: 'lower' ({constraint['lower']}) must be less than 'upper' ({constraint['upper']})"
                    )

                if constraint['step'] <= 0:
                    raise ValueError(
                        f"Parameter '{param_name}': constraint {i}: 'step' must be positive, got {constraint['step']}"
                    )

            return "int"

        else:
            raise ValueError(
                f"Parameter '{param_name}': constraint must have either 'values' (categorical) or 'step/lower/upper' (integer range)"
            )

    @staticmethod
    def validate_guardrails_v03(breeder_config):
        """Validate guardrails section

        Args:
            breeder_config: Full breeder configuration

        Raises:
            ValueError: If guardrails section is invalid
        """
        if 'guardrails' not in breeder_config:
            # Guardrails are optional
            return

        guardrails = breeder_config['guardrails']

        if not isinstance(guardrails, list):
            raise ValueError(
                f"'guardrails' must be a list, got {type(guardrails).__name__}"
            )

        for idx, guardrail in enumerate(guardrails):
            if not isinstance(guardrail, dict):
                raise ValueError(
                    f"Guardrail {idx}: must be a dict, got {type(guardrail).__name__}"
                )

            # Validate required fields
            if 'name' not in guardrail:
                raise ValueError(f"Guardrail {idx}: missing required field 'name'")

            if not isinstance(guardrail['name'], str) or guardrail['name'].strip() == "":
                raise ValueError(
                    f"Guardrail {idx}: 'name' must be a non-empty string"
                )

            if 'hard_limit' not in guardrail:
                raise ValueError(f"Guardrail {idx} ({guardrail['name']}): missing required field 'hard_limit'")

            if not isinstance(guardrail['hard_limit'], (int, float)):
                raise ValueError(
                    f"Guardrail {idx} ({guardrail['name']}): 'hard_limit' must be numeric, got {type(guardrail['hard_limit']).__name__}"
                )

            # Validate reconnaissance section
            if 'reconnaissance' not in guardrail:
                raise ValueError(f"Guardrail {idx} ({guardrail['name']}): missing required section 'reconnaissance'")

            recon = guardrail['reconnaissance']

            if not isinstance(recon, dict):
                raise ValueError(
                    f"Guardrail {idx} ({guardrail['name']}): 'reconnaissance' must be a dict, got {type(recon).__name__}"
                )

            # Validate reconnaissance fields
            required_recon_fields = ['service', 'query']
            missing_fields = [f for f in required_recon_fields if f not in recon]

            if missing_fields:
                raise ValueError(
                    f"Guardrail {idx} ({guardrail['name']}): 'reconnaissance' missing required fields: {', '.join(missing_fields)}"
                )

            # Validate service type
            valid_services = ['prometheus']  # Extend as needed
            if recon['service'] not in valid_services:
                raise ValueError(
                    f"Guardrail {idx} ({guardrail['name']}): service '{recon['service']}' not supported. "
                    f"Valid services: {', '.join(valid_services)}"
                )

            # Validate query is string
            if not isinstance(recon['query'], str) or recon['query'].strip() == "":
                raise ValueError(
                    f"Guardrail {idx} ({guardrail['name']}): 'query' must be a non-empty string"
                )

            # Validate optional numeric fields with defaults
            if 'stabilization_seconds' in recon:
                if not isinstance(recon['stabilization_seconds'], int) or recon['stabilization_seconds'] < 0:
                    raise ValueError(
                        f"Guardrail {idx} ({guardrail['name']}): 'stabilization_seconds' must be a non-negative integer"
                    )

            if 'samples' in recon:
                if not isinstance(recon['samples'], int) or recon['samples'] < 1:
                    raise ValueError(
                        f"Guardrail {idx} ({guardrail['name']}): 'samples' must be an integer >= 1"
                    )

            if 'interval' in recon:
                if not isinstance(recon['interval'], int) or recon['interval'] < 1:
                    raise ValueError(
                        f"Guardrail {idx} ({guardrail['name']}): 'interval' must be an integer >= 1"
                    )

    @staticmethod
    def validate_rollback_strategies_v03(breeder_config):
        """Validate rollback_strategies section

        Args:
            breeder_config: Full breeder configuration

        Raises:
            ValueError: If rollback_strategies section is invalid
        """
        if 'rollback_strategies' not in breeder_config:
            # Rollback strategies are optional
            return

        strategies = breeder_config['rollback_strategies']

        if not isinstance(strategies, dict):
            raise ValueError(
                f"'rollback_strategies' must be a dict, got {type(strategies).__name__}"
            )

        # Track strategy names for reference validation
        strategy_names = set()

        for strategy_name, strategy in strategies.items():
            if not isinstance(strategy, dict):
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': must be a dict, got {type(strategy).__name__}"
                )

            strategy_names.add(strategy_name)

            # Validate required fields
            required_fields = ['consecutive_failures', 'target_state', 'max_attempts', 'on_failure', 'timeout_seconds', 'after']
            missing_fields = [f for f in required_fields if f not in strategy]

            if missing_fields:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': missing required fields: {', '.join(missing_fields)}"
                )

            # Validate consecutive_failures
            if not isinstance(strategy['consecutive_failures'], int) or strategy['consecutive_failures'] < 1:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'consecutive_failures' must be an integer >= 1"
                )

            # Validate target_state
            valid_states = ['previous', 'best', 'baseline']
            if strategy['target_state'] not in valid_states:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'target_state' must be one of {valid_states}, got '{strategy['target_state']}'"
                )

            # Validate max_attempts
            if not isinstance(strategy['max_attempts'], int) or strategy['max_attempts'] < 1:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'max_attempts' must be an integer >= 1"
                )

            # Validate on_failure
            valid_failure_actions = ['stop', 'continue', 'skip_target']
            if strategy['on_failure'] not in valid_failure_actions:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'on_failure' must be one of {valid_failure_actions}, got '{strategy['on_failure']}'"
                )

            # Validate timeout_seconds
            if not isinstance(strategy['timeout_seconds'], int) or strategy['timeout_seconds'] < 1:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'timeout_seconds' must be an integer >= 1"
                )

            # Validate 'after' section
            after = strategy['after']
            if not isinstance(after, dict):
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'after' must be a dict, got {type(after).__name__}"
                )

            if 'action' not in after:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'after' section missing required field 'action'"
                )

            valid_after_actions = ['pause', 'continue', 'stop']
            if after['action'] not in valid_after_actions:
                raise ValueError(
                    f"Rollback strategy '{strategy_name}': 'after.action' must be one of {valid_after_actions}, got '{after['action']}'"
                )

            # If action is 'pause', 'duration' is required
            if after['action'] == 'pause':
                if 'duration' not in after:
                    raise ValueError(
                        f"Rollback strategy '{strategy_name}': 'after.duration' is required when 'after.action' is 'pause'"
                    )

                if not isinstance(after['duration'], int) or after['duration'] < 1:
                    raise ValueError(
                        f"Rollback strategy '{strategy_name}': 'after.duration' must be an integer >= 1"
                    )

        # Validate strategy references in targets
        if 'effectuation' in breeder_config and 'targets' in breeder_config['effectuation']:
            targets = breeder_config['effectuation']['targets']

            for idx, target in enumerate(targets):
                if 'rollback' in target and target['rollback'].get('enabled', False):
                    if 'strategy' not in target['rollback']:
                        raise ValueError(
                            f"Target {idx}: rollback.enabled=true but 'rollback.strategy' is not specified"
                        )

                    strategy_ref = target['rollback']['strategy']

                    if strategy_ref not in strategy_names:
                        raise ValueError(
                            f"Target {idx}: references undefined rollback strategy '{strategy_ref}'. "
                            f"Available strategies: {', '.join(sorted(strategy_names)) if strategy_names else 'none'}"
                        )

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

        Validation features:
        - ConfigVersion check (warns if outdated)
        - Guardrails and rollback_strategies validation
        - Constraint structure validation (list of ranges/values)
        - Support for sysctl, sysfs, cpufreq, ethtool categories
        - Aggregated error reporting with numbered errors
        - Empty/whitespace string validation
        - Positive value validation
        - Target field validation
        - Objective and guardrail reconnaissance validation
        - Run completion criteria validation
        """
        errors = []

        # Check ConfigVersion
        meta_section = breeder_config.get('meta', {})
        config_version = meta_section.get('configVersion', '0.2')
        if config_version != '0.3':
            errors.append(
                f"Config version '{config_version}' is outdated. Current version is '0.3'. "
                f"Please update your configuration to v0.3 format. "
                f"See documentation for migration guide."
            )

        if not breeder_config.get('breeder', {}).get('type'):
            errors.append(
                "Missing breeder.type. Example: breeder: {type: 'linux_performance'}"
            )

        if not breeder_config.get('objectives') or len(breeder_config.get('objectives', [])) == 0:
            errors.append(
                "Missing or empty objectives array. "
                "Example: objectives: [{name: 'latency', goal: 'MINIMIZE', reconnaissance: {...}}]"
            )

        if not breeder_config.get('effectuation', {}).get('targets') or len(breeder_config.get('effectuation', {}).get('targets', [])) == 0:
            errors.append(
                "Missing or empty effectuation.targets array. "
                "Example: effectuation: {targets: [{type: 'ssh', address: '1.2.3.4', ...}]}"
            )

        # Support multiple settings categories (sysctl, sysfs, cpufreq, ethtool)
        supported_categories = ['sysctl', 'sysfs', 'cpufreq', 'ethtool']
        settings = breeder_config.get('settings', {})

        # Check if at least one supported category exists
        has_any_category = any(category in settings for category in supported_categories)

        if not has_any_category:
            errors.append(
                f"Missing settings configuration. "
                f"At least one category required: {', '.join(supported_categories)}"
            )

        # Validate constraints structure for all present categories
        for category in supported_categories:
            if category in settings:
                if not isinstance(settings[category], dict):
                    errors.append(
                        f"settings.{category} must be a dict, got {type(settings[category]).__name__}"
                    )
                    continue

                # Validate each parameter's constraints
                for param_name, param_config in settings[category].items():
                    # Validate parameter name is not empty/whitespace
                    if not param_name or param_name.strip() == "":
                        errors.append(
                            f"settings.{category}: parameter name cannot be empty or whitespace"
                        )
                        continue

                    if not isinstance(param_config, dict):
                        errors.append(
                            f"settings.{category}.{param_name}: must be a dict, got {type(param_config).__name__}"
                        )
                        continue

                    if 'constraints' not in param_config:
                        errors.append(
                            f"settings.{category}.{param_name}: missing 'constraints' field. "
                            f"Example: constraints: [{{step: 100, lower: 4096, upper: 131072}}]"
                        )
                        continue

                    try:
                        # Validate constraint structure (list of ranges or values)
                        constraint_type = BreederConfig.validate_constraints_v03(
                            param_config['constraints'],
                            f"{category}.{param_name}"
                        )
                    except ValueError as e:
                        errors.append(str(e))

        # Validate guardrails section (if present)
        try:
            BreederConfig.validate_guardrails_v03(breeder_config)
        except ValueError as e:
            errors.append(str(e))

        # Validate rollback_strategies section (if present)
        try:
            BreederConfig.validate_rollback_strategies_v03(breeder_config)
        except ValueError as e:
            errors.append(str(e))

        # Validate cooperation configuration
        if breeder_config.get('cooperation', {}).get('active', False):
            parallel_workers = breeder_config.get('run', {}).get('parallel', 1)
            if parallel_workers <= 1:
                errors.append(
                    f"Cooperation enabled but run.parallel={parallel_workers}. "
                    "Cooperation requires parallel > 1 for multiple workers to share trials."
                )

        # Validate target type compatibility and required fields
        breeder_type = breeder_config.get('breeder', {}).get('type')
        if breeder_type in BREEDER_CAPABILITIES:
            supported_types = BREEDER_CAPABILITIES[breeder_type]['supported_target_types']

            for idx, target in enumerate(breeder_config.get('effectuation', {}).get('targets', [])):
                target_type = target.get('type')

                if not target_type:
                    errors.append(f"Target {idx}: Missing required 'type' field. Example: type: 'ssh'")
                elif target_type not in supported_types:
                    errors.append(
                        f"Target {idx} ({target.get('address', 'unknown')}): "
                        f"Type '{target_type}' not supported by breeder '{breeder_type}'. "
                        f"Supported types: {supported_types}"
                    )

                # Validate SSH-specific target fields
                if target_type == 'ssh':
                    # Check address
                    if 'address' not in target:
                        errors.append(
                            f"Target {idx}: Missing required field 'address' for SSH target. "
                            f"Example: address: '192.168.1.10'"
                        )
                    elif not isinstance(target['address'], str) or target['address'].strip() == "":
                        errors.append(
                            f"Target {idx}: 'address' must be a non-empty string"
                        )

                    # Check username
                    if 'username' not in target:
                        errors.append(
                            f"Target {idx}: Missing required field 'username' for SSH target. "
                            f"Example: username: 'admin'"
                        )
                    elif not isinstance(target['username'], str) or target['username'].strip() == "":
                        errors.append(
                            f"Target {idx}: 'username' must be a non-empty string"
                        )

                    # Check credential (either credentialName or credentialId)
                    has_credential_name = 'credentialName' in target
                    has_credential_id = 'credentialId' in target

                    if not has_credential_name and not has_credential_id:
                        errors.append(
                            f"Target {idx}: SSH target requires either 'credentialName' or 'credentialId'. "
                            f"Example: credentialName: 'my-ssh-key'"
                        )
                    elif has_credential_name and (not isinstance(target['credentialName'], str) or target['credentialName'].strip() == ""):
                        errors.append(
                            f"Target {idx}: 'credentialName' must be a non-empty string"
                        )
                    elif has_credential_id and (not isinstance(target['credentialId'], str) or target['credentialId'].strip() == ""):
                        errors.append(
                            f"Target {idx}: 'credentialId' must be a non-empty string"
                        )

        # Validate objectives reconnaissance configuration
        for idx, objective in enumerate(breeder_config.get('objectives', [])):
            if not isinstance(objective, dict):
                errors.append(
                    f"Objective {idx}: must be a dict, got {type(objective).__name__}"
                )
                continue

            # Validate reconnaissance section
            if 'reconnaissance' in objective:
                recon = objective['reconnaissance']

                if not isinstance(recon, dict):
                    errors.append(
                        f"Objective {idx}: 'reconnaissance' must be a dict, got {type(recon).__name__}"
                    )
                    continue

                # Check required fields
                required_recon_fields = ['service', 'query']
                missing_fields = [f for f in required_recon_fields if f not in recon]

                if missing_fields:
                    obj_name = objective.get('name', f'#{idx}')
                    errors.append(
                        f"Objective {idx} ({obj_name}): 'reconnaissance' missing required fields: {', '.join(missing_fields)}. "
                        f"Example: reconnaissance: {{service: 'prometheus', query: 'rate(...)'}}"
                    )

                # Validate service type
                if 'service' in recon:
                    valid_services = ['prometheus']  # Extend as needed
                    if recon['service'] not in valid_services:
                        obj_name = objective.get('name', f'#{idx}')
                        errors.append(
                            f"Objective {idx} ({obj_name}): service '{recon['service']}' not supported. "
                            f"Valid services: {', '.join(valid_services)}"
                        )

                # Validate query is non-empty string
                if 'query' in recon:
                    if not isinstance(recon['query'], str) or recon['query'].strip() == "":
                        obj_name = objective.get('name', f'#{idx}')
                        errors.append(
                            f"Objective {idx} ({obj_name}): 'query' must be a non-empty string"
                        )

                # Validate optional numeric fields
                if 'stabilization_seconds' in recon:
                    if not isinstance(recon['stabilization_seconds'], int) or recon['stabilization_seconds'] < 0:
                        obj_name = objective.get('name', f'#{idx}')
                        errors.append(
                            f"Objective {idx} ({obj_name}): 'stabilization_seconds' must be a non-negative integer"
                        )

                if 'samples' in recon:
                    if not isinstance(recon['samples'], int) or recon['samples'] < 1:
                        obj_name = objective.get('name', f'#{idx}')
                        errors.append(
                            f"Objective {idx} ({obj_name}): 'samples' must be an integer >= 1"
                        )

                if 'interval' in recon:
                    if not isinstance(recon['interval'], int) or recon['interval'] < 1:
                        obj_name = objective.get('name', f'#{idx}')
                        errors.append(
                            f"Objective {idx} ({obj_name}): 'interval' must be an integer >= 1"
                        )

        # Validate run completion criteria
        run_config = breeder_config.get('run', {})
        if 'completion' in run_config:
            completion = run_config['completion']

            if not isinstance(completion, dict):
                errors.append(
                    f"'run.completion' must be a dict, got {type(completion).__name__}"
                )
            else:
                # Validate iterations criteria
                if 'iterations' in completion:
                    iterations = completion['iterations']

                    if not isinstance(iterations, dict):
                        errors.append(
                            f"'run.completion.iterations' must be a dict, got {type(iterations).__name__}"
                        )
                    else:
                        # Check min <= max
                        iterations_min = iterations.get('min')
                        iterations_max = iterations.get('max')

                        if iterations_min is not None and iterations_max is not None:
                            if not isinstance(iterations_min, int) or iterations_min < 1:
                                errors.append(
                                    f"'run.completion.iterations.min' must be an integer >= 1"
                                )
                            elif not isinstance(iterations_max, int) or iterations_max < 1:
                                errors.append(
                                    f"'run.completion.iterations.max' must be an integer >= 1"
                                )
                            elif iterations_min > iterations_max:
                                errors.append(
                                    f"'run.completion.iterations.min' ({iterations_min}) must be <= 'max' ({iterations_max})"
                                )

        if errors:
            # Numbered error list for better readability
            error_count = len(errors)
            plural = "s" if error_count > 1 else ""
            error_msg = f"Config validation failed ({error_count} error{plural}):\n"
            error_msg += "\n".join(f"  [{i+1}/{error_count}] {err}" for i, err in enumerate(errors))
            raise ValueError(error_msg)

        return True