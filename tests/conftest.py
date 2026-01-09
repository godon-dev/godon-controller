#
# Copyright (c) 2019 Matthias Tafelmeier.
#
# This file is part of godon
#
# godon is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# godon is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this godon. If not, see <http://www.gnu.org/licenses/>.
#

import pytest
import sys
import os
import types
from unittest.mock import MagicMock

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock external dependencies before imports
sys.modules['database'] = MagicMock()

# Create stub modules using types.ModuleType
def create_stub_module(name):
    module = types.ModuleType(name)
    module.__path__ = []
    module.__spec__ = None
    module.__name__ = name
    return module

# Create f and f.controller
fake_f = create_stub_module('f')
fake_controller = create_stub_module('f.controller')
sys.modules['f'] = fake_f
sys.modules['f.controller'] = fake_controller

# Pre-populate all f.controller.xxx modules BEFORE any imports
for module_name in ['config', 'database', 'breeder_service', 'credential_create',
                    'credential_get', 'credential_delete', 'credentials_get']:
    full_name = f'f.controller.{module_name}'
    stub = create_stub_module(full_name)
    sys.modules[full_name] = stub

# Helper function to copy module contents
def populate_stub_module(stub_module, source_module):
    """Copy all attributes from source_module to stub_module"""
    for attr_name in dir(source_module):
        if not attr_name.startswith('_'):
            setattr(stub_module, attr_name, getattr(source_module, attr_name))

# Import controller modules in dependency order - config first, then others
# IMPORTANT: Import and populate immediately to avoid circular import issues
import controller.config as config
populate_stub_module(sys.modules['f.controller.config'], config)

import controller.database as database
populate_stub_module(sys.modules['f.controller.database'], database)

import controller.breeder_service as breeder_service
populate_stub_module(sys.modules['f.controller.breeder_service'], breeder_service)

import controller.credential_create as credential_create
populate_stub_module(sys.modules['f.controller.credential_create'], credential_create)

import controller.credential_get as credential_get
populate_stub_module(sys.modules['f.controller.credential_get'], credential_get)

import controller.credential_delete as credential_delete
populate_stub_module(sys.modules['f.controller.credential_delete'], credential_delete)

import controller.credentials_get as credentials_get
populate_stub_module(sys.modules['f.controller.credentials_get'], credentials_get)