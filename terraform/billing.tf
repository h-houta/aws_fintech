# ── Billing Alarms ───────────────────────────────────────────────────────────
# Alerts at $5, $10, and $50 thresholds to prevent surprise charges.
# Billing metrics are only available in us-east-1 (our default region).

resource "aws_cloudwatch_metric_alarm" "billing_5" {
  alarm_name          = "billing-alarm-5-usd"
  alarm_description   = "Estimated charges have exceeded $5"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  statistic           = "Maximum"
  period              = 21600 # 6 hours
  evaluation_periods  = 1
  threshold           = 5
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "billing_10" {
  alarm_name          = "billing-alarm-10-usd"
  alarm_description   = "Estimated charges have exceeded $10"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  statistic           = "Maximum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = 10
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "billing_50" {
  alarm_name          = "billing-alarm-50-usd"
  alarm_description   = "Estimated charges have exceeded $50"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  statistic           = "Maximum"
  period              = 21600
  evaluation_periods  = 1
  threshold           = 50
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Currency = "USD"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}
