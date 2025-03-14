import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as s3d from "aws-cdk-lib/aws-s3-deployment";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
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
    const applicationName = "batch-email-service";

    const awsRegion = stack.region;
    const accountId = stack.account;

    // DDB Tables:
    const TemplateMetadataTable = new ddb.TableV2(
      stack,
      "TemplateMetadataTable",
      {
        tableName: `${applicationName}_template-metadata`,
        partitionKey: {
          name: "template_key",
          type: ddb.AttributeType.STRING,
        },
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    const EmailBatchTrackerTable = new ddb.TableV2(
      stack,
      "EmailBatchTrackerTable",
      {
        tableName: `${applicationName}_email-batch-tracker`,
        partitionKey: {
          name: "batch_name",
          type: ddb.AttributeType.STRING,
        },
        timeToLiveAttribute: "expirationTime",
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    // SQS Queues:
    const failedEmailBatchQueue = new sqs.Queue(stack, "EmailBatchDLQ", {
      queueName: `${applicationName}_email-batch-dlq`,
      retentionPeriod: cdk.Duration.days(14),
    });

    const emailBatchQueue = new sqs.Queue(stack, "EmailBatchQueue", {
      queueName: `${applicationName}_email-batch-queue`,
      deadLetterQueue: {
        queue: failedEmailBatchQueue,
        maxReceiveCount: 1,
      },
      visibilityTimeout: cdk.Duration.minutes(1),
      retentionPeriod: cdk.Duration.minutes(30),
    });

    // S3 Buckets:
    const s3BucketLifeCycleRule = {
      abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
      expiredObjectDeleteMarker: true,
    };

    const jcBatchEmailServiceBucket = new s3.Bucket(
      stack,
      "BatchEmailServiceBucket",
      {
        bucketName: `${applicationName}-resource-bucket`,
        lifecycleRules: [s3BucketLifeCycleRule],
        versioned: true,
        autoDeleteObjects: false,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      }
    );

    // Deploys initial asset files to S3 bucket
    new s3d.BucketDeployment(stack, "InitialAssetDeployment", {
      destinationBucket: jcBatchEmailServiceBucket,
      sources: [s3d.Source.asset(path.join(__dirname, "../assets"))],
      exclude: ["**/.DS_Store", "*.DS_Store"],
    });

    // IAM:
    const sendBatchEmailEventRole = new iam.Role(
      stack,
      "SendBatchEmailEventRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      }
    );
    const sendBatchEmailEventRolePolicies = [
      {
        actions: ["s3:GetObject", "s3:DeleteObject", "s3:PutObject"],
        resources: [`${jcBatchEmailServiceBucket.bucketArn}/*`],
      },
      {
        actions: ["ses:SendRawEmail"],
        resources: [process.env.SES_IDENTITY_DOMAIN_ARN!],
      },
      {
        actions: ["dynamodb:GetItem"],
        resources: [TemplateMetadataTable.tableArn],
      },
      {
        actions: ["dynamodb:PutItem"],
        resources: [EmailBatchTrackerTable.tableArn],
      },
    ];
    addPolicyToLambdaRole(
      sendBatchEmailEventRolePolicies,
      sendBatchEmailEventRole
    );

    const processSesTemplateRole = new iam.Role(
      stack,
      "ProcessSesTemplateRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      }
    );
    const processSesTemplateRolePolicies = [
      {
        actions: ["s3:GetObject", "s3:PutObject"],
        resources: [`${jcBatchEmailServiceBucket.bucketArn}/*`],
      },
      {
        actions: ["ses:SendRawEmail"],
        resources: [process.env.SES_IDENTITY_DOMAIN_ARN!],
      },
      {
        actions: ["dynamodb:PutItem", "dynamodb:GetItem"],
        resources: [TemplateMetadataTable.tableArn],
      },
    ];
    addPolicyToLambdaRole(
      processSesTemplateRolePolicies,
      processSesTemplateRole
    );

    //// ProcessBatchEmailEvent Lambda Execution Roles:
    const processBatchEmailEventRole = new iam.Role(
      stack,
      "ProcessBatchEmailEventRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      }
    );
    const processBatchEmailEventRolePolicies = [
      {
        actions: ["s3:GetObject"],
        resources: [`${jcBatchEmailServiceBucket.bucketArn}/*`],
      },
      {
        actions: ["ses:SendRawEmail"],
        resources: [process.env.SES_IDENTITY_DOMAIN_ARN!],
      },
      {
        actions: ["dynamodb:UpdateItem"],
        resources: [EmailBatchTrackerTable.tableArn],
      },
    ];
    addPolicyToLambdaRole(
      processBatchEmailEventRolePolicies,
      processBatchEmailEventRole
    );

    const scheduleBatchEmailRole = new iam.Role(
      stack,
      "ScheduleBatchEmailRole",
      {
        assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      }
    );
    const scheduleBatchEmailRolePolicies = [
      {
        actions: [
          "scheduler:CreateSchedule",
          "scheduler:PutTargets",
          "scheduler:DeleteSchedule",
          "scheduler:TagResource",
        ],
        resources: [`arn:aws:scheduler:${awsRegion}:${accountId}:schedule/*`],
      },
    ];
    addPolicyToLambdaRole(
      scheduleBatchEmailRolePolicies,
      scheduleBatchEmailRole
    );

    // Lambdas:
    const sendBatchEmailEvent = new lambda.Function(
      stack,
      "SendBatchEmailEvent",
      {
        functionName: `${applicationName}_send-batch-email-event`,
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/send_batch_email_event"),
          {
            exclude: ["**/__pycache__/*", "__pycache__"],
          }
        ),
        environment: {
          BATCH_EMAIL_SERVICE_BUCKET_NAME: jcBatchEmailServiceBucket.bucketName,
          BATCH_INITIATION_ERROR_S3_PREFIX: "batch/archive/error/",
          SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY:
            "templates/system/send-batch-failure-email-template.html",
          SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY:
            "templates/system/send-batch-failure-email-template.txt",
          SES_NO_REPLY_SENDER: "no-reply@johnjhc.com",
          SES_ADMIN_EMAIL: "jchoi950@yahoo.com",
          EMAIL_BATCH_QUEUE_NAME: emailBatchQueue.queueName,
          RECIPIENTS_PER_MESSAGE: "50",
          LOG_LEVEL: "INFO",
          EMAIL_REQUIRED_FIELDS: process.env.EMAIL_REQUIRED_FIELDS!,
          TEMPLATE_METADATA_TABLE_NAME: TemplateMetadataTable.tableName,
        },
        role: sendBatchEmailEventRole,
      }
    );

    const processSesTemplate = new lambda.Function(
      stack,
      "ProcessSesTemplate",
      {
        functionName: `${applicationName}_process-ses-template-event`,
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/process_ses_template"),
          {
            exclude: ["**/__pycache__/*", "__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
          SES_NO_REPLY_SENDER: "no-reply@johnjhc.com",
          SES_ADMIN_EMAIL: "jchoi950@yahoo.com",
          BATCH_EMAIL_SERVICE_BUCKET_NAME: jcBatchEmailServiceBucket.bucketName,
          TEMPLATE_METADATA_TABLE_NAME: TemplateMetadataTable.tableName,
          PROCESS_SES_TEMPLATE_FAILURE_HTML_TEMPLATE_KEY:
            process.env.PROCESS_SES_TEMPLATE_FAILURE_HTML_TEMPLATE_KEY!,
          PROCESS_SES_TEMPLATE_FAILURE_TEXT_TEMPLATE_KEY:
            process.env.PROCESS_SES_TEMPLATE_FAILURE_TEXT_TEMPLATE_KEY!,
        },
        role: processSesTemplateRole,
      }
    );

    const processBatchEmailEvent = new lambda.Function(
      stack,
      "ProcessBatchEmailEvent",
      {
        functionName: `${applicationName}_process-batch-email-event`,
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/process_batch_email_event"),
          {
            exclude: ["**/__pycache__/*", "__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
        },
        role: processBatchEmailEventRole,
      }
    );
    processBatchEmailEvent.addEventSource(
      new eventSources.SqsEventSource(emailBatchQueue, {
        batchSize: 1,
      })
    );

    const scheduleBatchEmail = new lambda.Function(
      stack,
      "ScheduleBatchEmail",
      {
        functionName: `${applicationName}_schedule-batch-email`,
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "main.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(__dirname, "../lambdas/python/schedule_batch_email"),
          {
            exclude: ["**/__pycache__/*", "__pycache__"],
          }
        ),
        environment: {
          LOG_LEVEL: "INFO",
        },
        role: scheduleBatchEmailRole,
      }
    );

    // Grant SQS Send and Consume Permissions:
    failedEmailBatchQueue.grantSendMessages(processBatchEmailEvent);
    emailBatchQueue.grantSendMessages(sendBatchEmailEvent);
    emailBatchQueue.grantConsumeMessages(processBatchEmailEvent);

    // S3 Event Notification Configurations
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

function addPolicyToLambdaRole(
  policies: Array<cdk.aws_iam.PolicyStatementProps>,
  role: cdk.aws_iam.Role
) {
  role.addManagedPolicy(
    iam.ManagedPolicy.fromAwsManagedPolicyName(
      //// AWS managed basic lambda execution role
      "service-role/AWSLambdaBasicExecutionRole"
    )
  );

  for (const policy of policies) {
    role.addToPolicy(new iam.PolicyStatement(policy));
  }
}
