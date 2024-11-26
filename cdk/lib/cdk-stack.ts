import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as s3d from "aws-cdk-lib/aws-s3-deployment";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const app = new cdk.App();
    const stack = new cdk.Stack(app, "MainStack");

    // S3 Buckets:
    const s3BucketLifeCycleRule: s3.LifecycleRule = {
      abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
      expiredObjectDeleteMarker: true,
    };

    const jcBatchEmailServiceBucket: s3.Bucket = new s3.Bucket(
      stack,
      "jcBatchEmailServiceBucket",
      {
        lifecycleRules: [s3BucketLifeCycleRule],
        versioned: true,
        autoDeleteObjects: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    // Lambdas:
    const sendBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "sendBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda.handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/send_batch_email_event"
          )
        ),
      }
    );

    const processBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "processBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda.handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/process_ses_template"
          )
        ),
      }
    );

    // S3 Event Notification Configuration
    const eventNotificationConfiguration = [
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(sendBatchEmailEvent),
        filters: {
          prefix: "send-list/*",
          suffix: ".csv",
        },
      },
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(processBatchEmailEvent),
        filters: {
          prefix: "templates/*",
          suffix: ".html",
        },
      },
      {
        eventType: s3.EventType.OBJECT_REMOVED,
        getDestination: () => new s3n.LambdaDestination(processBatchEmailEvent),
        filters: {
          prefix: "templates/*",
          suffix: ".html",
        },
      },
    ];

    eventNotificationConfiguration.forEach((config) => {
      jcBatchEmailServiceBucket.addEventNotification(
        config.eventType,
        config.getDestination(),
        config.filters
      );
    });

    // Deploy initial asset files to S3 bucket
    new s3d.BucketDeployment(stack, "DeployInitialAssets", {
      destinationBucket: jcBatchEmailServiceBucket,
      sources: [s3d.Source.asset(path.join(__dirname, "../assets"))],
      destinationKeyPrefix: "assets/",
    });
  }
}
