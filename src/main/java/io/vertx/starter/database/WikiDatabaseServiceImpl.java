package io.vertx.starter.database;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLConnection;
import rx.Observable;
import rx.Single;

import java.util.HashMap;
import java.util.List;

/**
 * Created by wujun on 2017/7/19.
 */
public class WikiDatabaseServiceImpl implements WikiDatabaseService {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);

  private final HashMap<SqlQuery, String> sqlQueries;
  private final JDBCClient dbClient;

  /**
   *
   *
   * @param dbClient
   * @param sqlQueries
   * @param readyHandler
   */
  WikiDatabaseServiceImpl(io.vertx.ext.jdbc.JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries,
                          Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
    this.dbClient = new JDBCClient(dbClient);
    this.sqlQueries = sqlQueries;

    getConnection()
      .flatMap(conn -> conn.rxExecute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE)))
      .map(v -> this)
      .subscribe(RxHelper.toSubscriber(readyHandler));
  }

  /**
   *
   *
   * @return
   */
  private Single<SQLConnection> getConnection() {
    return dbClient.rxGetConnection().flatMap(conn -> {
      Single<SQLConnection> connectionSingle = Single.just(conn);
      return connectionSingle.doOnUnsubscribe(conn::close);
    });
  }

  @Override
  public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES)))
      .flatMapObservable(res -> {
        List<JsonArray> results = res.getResults();
        return Observable.from(results);
      })
      .map(json -> json.getString(0))
      .sorted()
      .collect(JsonArray::new, JsonArray::add)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxQueryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), new JsonArray().add(name)))
      .map(result -> {
        if (result.getNumRows() > 0) {
          JsonArray row = result.getResults().get(0);
          return new JsonObject()
            .put("found", true)
            .put("id", row.getInteger(0))
            .put("rawContent", row.getString(1));
        } else {
          return new JsonObject().put("found", false);
        }
      })
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService fetchPageById(int id, Handler<AsyncResult<JsonObject>> resultHandler) {
    Single<SQLConnection> connection = getConnection();
    Single<ResultSet> resultSet = connection
      .flatMap(conn -> conn.rxQueryWithParams(sqlQueries.get(SqlQuery.GET_PAGE_BY_ID), new JsonArray().add(id)));
    resultSet
      .map(result -> {
        if (result.getNumRows() > 0) {
          JsonObject row = result.getRows().get(0);
          return new JsonObject()
            .put("found", true)
            .put("id", row.getInteger("ID"))
            .put("name", row.getString("NAME"))
            .put("content", row.getString("CONTENT"));
        } else {
          return new JsonObject().put("found", false);
        }
      })
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxUpdateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), new JsonArray().add(title).add(markdown)))
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(conn -> conn.rxUpdateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), new JsonArray().add(markdown).add(id)))
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
    getConnection()
      .flatMap(connection -> {
        JsonArray data = new JsonArray().add(id);
        return connection.rxUpdateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data);
      })
      .map(res -> (Void) null)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }

  @Override
  public WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler) {
    getConnection()
      .flatMap(connection -> connection.rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES_DATA)))
      .map(ResultSet::getRows)
      .subscribe(RxHelper.toSubscriber(resultHandler));
    return this;
  }
}


