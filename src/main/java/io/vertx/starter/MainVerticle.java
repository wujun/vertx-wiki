package io.vertx.starter;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.rxjava.core.AbstractVerticle;
import rx.Single;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single<String> dbVerticleDeployment = vertx.rxDeployVerticle(
      "io.vertx.starter.database.WikiDatabaseVerticle");

    dbVerticleDeployment.flatMap(id -> {
      Single<String> httpVerticleDeployment = vertx.rxDeployVerticle(
        "io.vertx.starter.http.HttpServerVerticle",
        new DeploymentOptions().setInstances(2));

      return httpVerticleDeployment;
    }).subscribe(id -> startFuture.complete(), startFuture::fail);
  }
}
