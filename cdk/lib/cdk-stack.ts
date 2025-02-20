import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as s3d from "aws-cdk-lib/aws-s3-deployment";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as eventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as path from "path";
import * as dotenv from "dotenv";

dotenv.config({
  path: path.resolve(__dirname, "../lambdas/python/.env"),
});

type S3EventNotificationDestinations =
  | s3n.LambdaDestination
  | s3n.SnsDestination
  | s3n.SqsDestination;

interface EventNotificationConfiguration {
  eventType: s3.EventType;
  getDestination: () => S3EventNotificationDestinations;
  filters: {
    prefix?: string;
    suffix?: string;
  };
}

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const app = new cdk.App();
    const stack = new cdk.Stack(app, "BatchEmailServiceMainStack");

    const awsRegion = stack.region;
    const accountId = stack.account;

    // SQS Queues:
    const failedEmailBatchQueue: sqs.Queue = new sqs.Queue(
      stack,
      "EmailBatchDLQ",
      {
        queueName: "EmailBatchDLQ",
        retentionPeriod: cdk.Duration.days(14),
      }
    );

    const emailBatchQueue: sqs.Queue = new sqs.Queue(stack, "EmailBatchQueue", {
      queueName: "EmailBatchQueue",
      deadLetterQueue: {
        queue: failedEmailBatchQueue,
        maxReceiveCount: 1,
      },
      visibilityTimeout: cdk.Duration.minutes(1),
      retentionPeriod: cdk.Duration.minutes(30),
    });

    // S3 Buckets:
    const s3BucketLifeCycleRule: s3.LifecycleRule = {
      abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
      expiredObjectDeleteMarker: true,
    };

    const jcBatchEmailServiceBucket: s3.Bucket = new s3.Bucket(
      stack,
      "BatchEmailServiceBucket",
      {
        lifecycleRules: [s3BucketLifeCycleRule],
        versioned: true,
        autoDeleteObjects: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    // Deploy initial asset files to S3 bucket
    const batchEmailBucket = new s3d.BucketDeployment(
      stack,
      "DeployInitialAssets",
      {
        destinationBucket: jcBatchEmailServiceBucket,
        sources: [s3d.Source.asset(path.join(__dirname, "../assets"))],
        exclude: ["**/.DS_Store", "*.DS_Store"],
      }
    );

    // // IAM Policies:
    // sendBatchEmailEvent Lambda Execution Roles:
    const sendBatchEmailEventRole = new iam.Role(
      stack,
      "SendBatchEmailEventRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      }
    );
    // AWS managed basic lambda execution role
    sendBatchEmailEventRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSLambdaBasicExecutionRole"
      )
    );
    // S3 policy
    sendBatchEmailEventRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:DeleteObject", "s3:PutObject"],
        resources: [`${batchEmailBucket.deployedBucket.bucketArn}/*`],
      })
    );
    // SES policy
    sendBatchEmailEventRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["ses:SendRawEmail"],
        resources: [process.env.SES_IDENTITY_DOMAIN_ARN!],
      })
    );

    // Lambdas:
    const sendBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "SendBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "send_batch_email_event.main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/send_batch_email_event/"),
          {
            exclude: ["**/__pycache__"],
          }
        ),
        environment: {
          BATCH_EMAIL_SERVICE_BUCKET_NAME: jcBatchEmailServiceBucket.bucketName,
          BATCH_INITIATION_ERROR_S3_PREFIX: "batch/archive/error/",
          SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY:
            "templates/send-batch-failure-email-template.html",
          SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY:
            "templates/send-batch-failure-email-template.txt",
          SES_NO_REPLY_SENDER: "no-reply@johnjhc.com",
          SES_ADMIN_EMAIL: "jchoi950@yahoo.com",
          EMAIL_BATCH_QUEUE_NAME: emailBatchQueue.queueName,
          RECIPIENTS_PER_MESSAGE: "50",
          LOG_LEVEL: "INFO",
          EMAIL_REQUIRED_FIELDS: process.env.EMAIL_REQUIRED_FIELDS!,
          AWS_DEFAULT_REGION: process.env.AWS_DEFAULT_REGION!,
          TEMPLATE_METADATA_TABLE_NAME:
            process.env.TEMPLATE_METADATA_TABLE_NAME!,
        },
        role: sendBatchEmailEventRole,
      }
    );

    const processBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "ProcessBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "process_batch_email_event.main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/process_batch_email_event/"),
          {
            exclude: ["**/__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
        },
      }
    );
    processBatchEmailEvent.addEventSource(
      new eventSources.SqsEventSource(emailBatchQueue, {
        batchSize: 1,
      })
    );

    const scheduleBatchEmail: lambda.Function = new lambda.Function(
      stack,
      "ScheduleBatchEmail",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "schedule_batch_lambda.main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/schedule_batch_email/"),
          {
            exclude: ["**/__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
        },
      }
    );
    scheduleBatchEmail.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "scheduler:CreateSchedule",
          "scheduler:PutTargets",
          "scheduler:DeleteSchedule",
          "scheduler:TagResource",
        ],
        resources: [`arn:aws:scheduler:${awsRegion}:${accountId}:schedule/*`],
      })
    );

    const processSesTemplate: lambda.Function = new lambda.Function(
      stack,
      "ProcessSesTemplate",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "process_ses_template.main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/process_ses_template/"),
          {
            exclude: ["**/__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
          AWS_DEFAULT_REGION: "",
        },
      }
    );

    // Grant SQS Send and Consume Permissions:
    failedEmailBatchQueue.grantSendMessages(processBatchEmailEvent);
    emailBatchQueue.grantSendMessages(sendBatchEmailEvent);
    emailBatchQueue.grantConsumeMessages(processBatchEmailEvent);

    // S3 Event Notification Configuration
    const eventNotificationConfigurations: EventNotificationConfiguration[] = [
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(sendBatchEmailEvent),
        filters: {
          prefix: "batch/send/",
          suffix: ".csv",
        },
      },
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(scheduleBatchEmail),
        filters: {
          prefix: "batch/scheduled/",
          suffix: ".csv",
        },
      },
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(processSesTemplate),
        filters: {
          prefix: "templates/",
          suffix: ".html",
        },
      },
      {
        eventType: s3.EventType.OBJECT_REMOVED,
        getDestination: () => new s3n.LambdaDestination(processSesTemplate),
        filters: {
          prefix: "templates/",
          suffix: ".html",
        },
      },
    ];

    eventNotificationConfigurations.forEach((config) => {
      jcBatchEmailServiceBucket.addEventNotification(
        config.eventType,
        config.getDestination(),
        config.filters
      );
    });
  }
}
