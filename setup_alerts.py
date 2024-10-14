from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import (
    AlertRuleResource,
    RuleCondition,
    ThresholdRuleCondition,
    SingleResourceMultipleMetricCriteria,
    MetricAlertResource,
    MetricAlertSingleResourceMultipleMetricCriteria,
    ActionGroup,
    RuleEmailAction
)
import os

def create_action_group(client, resource_group, action_group_name, email):
    """Create an action group for alerts."""
    action_group = ActionGroup(
        location='Global',
        group_short_name='alertgrp',
        email_receivers=[{
            'name': 'adminEmail',
            'email_address': email
        }]
    )
    client.action_groups.create_or_update(resource_group, action_group_name, action_group)
    print(f"Action Group '{action_group_name}' created.")

def create_metric_alert(client, resource_group, alert_name, action_group_id, function_app_id):
    """Create a metric alert for function app failures."""
    alert = MetricAlertResource(
        location='Global',
        description='Alert for Function App Failures',
        severity=3,
        enabled=True,
        scopes=[function_app_id],
        evaluation_frequency='PT5M',
        window_size='PT5M',
        criteria=SingleResourceMultipleMetricCriteria(
            all_of=[
                RuleCondition(
                    metric_name='FunctionExecutionUnits',
                    metric_namespace='Azure.Functions',
                    operator='GreaterThan',
                    threshold=100,  # Example threshold
                    time_aggregation='Total'
                )
            ]
        ),
        actions=[{
            'action_group_id': action_group_id
        }]
    )
    client.metric_alerts.create_or_update(resource_group, alert_name, alert)
    print(f"Metric Alert '{alert_name}' created.")

def main():
    # Configuration
    subscription_id = 'your-subscription-id'
    resource_group = 'your-resource-group'
    action_group_name = 'FunctionAppActionGroup'
    alert_name = 'FunctionAppFailureAlert'
    function_app_id = '/subscriptions/your-subscription-id/resourceGroups/your-resource-group/providers/Microsoft.Web/sites/your-function-app'  # Replace with your Function App ID
    admin_email = 'admin@example.com'
    
    # Authenticate
    credential = DefaultAzureCredential()
    monitor_client = MonitorManagementClient(credential, subscription_id)
    
    # Create Action Group
    create_action_group(monitor_client, resource_group, action_group_name, admin_email)
    
    # Get Action Group ID
    action_group = monitor_client.action_groups.get(resource_group, action_group_name)
    action_group_id = action_group.id
    
    # Create Metric Alert
    create_metric_alert(monitor_client, resource_group, alert_name, action_group_id, function_app_id)

if __name__ == "__main__":
    main()