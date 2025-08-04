# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, status

from airflow import plugins_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.config import ConfigResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_authenticated
from airflow.configuration import conf
from airflow.settings import DASHBOARD_UIALERTS
from airflow.utils.log.log_reader import TaskLogReader

logger = logging.getLogger(__name__)

config_router = AirflowRouter(tags=["Config"])


API_CONFIG_KEYS = [
    "enable_swagger_ui",
    "hide_paused_dags_by_default",
    "page_size",
    "default_wrap",
    "auto_refresh_interval",
    "require_confirmation_dag_change",
]


@config_router.get(
    "/config",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_authenticated())],
)
def get_configs(user: GetUserDep) -> ConfigResponse:
    """Get configs for UI."""
    config = {key: conf.get("api", key) for key in API_CONFIG_KEYS}

    # Initialize plugins to ensure menu items are loaded
    plugins_manager.initialize_flask_plugins()
    plugins_manager.initialize_ui_plugins()

    # Collect plugin menu items from both appbuilder_menu_items (deprecated but kept for backward compatibility)
    # and external_views with destination "nav" or None
    plugins_extra_menu_items = []

    # Add appbuilder_menu_items for backward compatibility
    # Once the plugin manager is initialized all its None attributes will be replaced with an empty list
    if plugins_manager.flask_appbuilder_menu_links:
        plugins_extra_menu_items.extend(plugins_manager.flask_appbuilder_menu_links)

    # Add external_views that have destination "nav" or None (which defaults to "nav")
    # external_views is preferred over appbuilder_menu_items
    if plugins_manager.external_views:
        for external_view in plugins_manager.external_views:
            destination = external_view.get("destination")
            if destination is None or destination == "nav":
                # Convert external_view to AppBuilderMenuItemResponse format
                # For external views, we need to construct the href from url_route if href is not present
                href = external_view.get("href")
                if not href and external_view.get("url_route"):
                    href = f"/plugin/{external_view['url_route']}"
                elif not href:
                    # Skip if no href and no url_route
                    continue

                menu_item = {
                    "name": external_view["name"],
                    "href": href,
                    "category": external_view.get("category")
                }
                plugins_extra_menu_items.append(menu_item)

    # Add react_apps that have destination "nav" or None (which defaults to "nav")
    if plugins_manager.react_apps:
        for react_app in plugins_manager.react_apps:
            destination = react_app.get("destination")
            if destination is None or destination == "nav":
                # Convert react_app to AppBuilderMenuItemResponse format
                # For react apps, we construct the href from url_route
                url_route = react_app.get("url_route")
                if not url_route:
                    # Skip if no url_route
                    continue

                href = f"/plugin/{url_route}"
                menu_item = {
                    "name": react_app["name"],
                    "href": href,
                    "category": react_app.get("category")
                }
                plugins_extra_menu_items.append(menu_item)

    # Collect plugin import errors to avoid 403 errors for users without plugin permissions
    plugin_import_errors = []
    if plugins_manager.import_errors:
        plugin_import_errors = [
            {"source": source, "error": error}
            for source, error in plugins_manager.import_errors.items()
        ]

    task_log_reader = TaskLogReader()
    additional_config: dict[str, Any] = {
        "instance_name": conf.get("api", "instance_name", fallback="Airflow"),
        "test_connection": conf.get("core", "test_connection", fallback="Disabled"),
        "dashboard_alert": DASHBOARD_UIALERTS,
        "show_external_log_redirect": task_log_reader.supports_external_link,
        "external_log_name": getattr(task_log_reader.log_handler, "log_name", None),
        "plugins_extra_menu_items": plugins_extra_menu_items,
        "plugin_import_errors": plugin_import_errors,
    }

    config.update({key: value for key, value in additional_config.items()})

    return ConfigResponse.model_validate(config)
