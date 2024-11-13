build:
	@echo "Building..."
	@sbt compile

run:
	@echo "Running..."
	@sbt run

deploy:
	@echo "Deploying..."
	@spark-submit --deploy-mode cluster --class edu.nyu.yx3494.Main target/scala-2.12/scr_2.12-0.1.0-SNAPSHOT.jar

clean:
	@echo "Cleaning..."
	@sbt clean

.PHONY: build run deploy clean