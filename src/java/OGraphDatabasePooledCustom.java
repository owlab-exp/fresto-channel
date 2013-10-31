/**************************************************************************************
 * Copyright 2013 TheSystemIdeas, Inc and Contributors. All rights reserved.          *
 *                                                                                    *
 *     https://github.com/owlab/fresto                                                *
 *                                                                                    *
 *                                                                                    *
 * ---------------------------------------------------------------------------------- *
 * This file is licensed under the Apache License, Version 2.0 (the "License");       *
 * you may not use this file except in compliance with the License.                   *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 * 
 **************************************************************************************/
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabasePooled;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.exception.ODatabaseException;

@SuppressWarnings("unchecked")
public class OGraphDatabasePooledCustom extends OGraphDatabase implements ODatabasePooled {

  private OGraphDatabasePoolCustom ownerPool;

  public OGraphDatabasePooledCustom(final OGraphDatabasePoolCustom iOwnerPool, final String iURL, final String iUserName,
      final String iUserPassword) {
    super(iURL);
    ownerPool = iOwnerPool;
    super.open(iUserName, iUserPassword);
  }

  public void reuse(final Object iOwner, final Object[] iAdditionalArgs) {
    ownerPool = (OGraphDatabasePoolCustom) iOwner;
    if (isClosed())
      open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);
    getLevel1Cache().invalidate();
    getMetadata().reload();
    checkForGraphSchema();
  }

  @Override
  public OGraphDatabasePooledCustom open(String iUserName, String iUserPassword) {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OGraphDatabase instance if you want to manually open the connection");
  }

  @Override
  public OGraphDatabasePooledCustom create() {
    throw new UnsupportedOperationException(
        "Database instance was retrieved from a pool. You cannot open the database in this way. Use directly a OGraphDatabase instance if you want to manually open the connection");
  }

  public boolean isUnderlyingOpen() {
    return !super.isClosed();
  }

  @Override
  public boolean isClosed() {
    return ownerPool == null || super.isClosed();
  }

  /**
   * Avoid to close it but rather release itself to the owner pool.
   */
  @Override
  public void close() {
    if (isClosed())
      return;

    vertexBaseClass = null;
    edgeBaseClass = null;

    checkOpeness();
    rollback();

    getMetadata().close();
    ((ODatabaseRaw) underlying.getUnderlying()).callOnCloseListeners();
    getLevel1Cache().clear();

    final OGraphDatabasePoolCustom pool = ownerPool;
    ownerPool = null;
    pool.release(this);
  }

  public void forceClose() {
    super.close();
  }

  @Override
  protected void checkOpeness() {
    if (ownerPool == null)
      throw new ODatabaseException(
          "Database instance has been released to the pool. Get another database instance from the pool with the right username and password");

    super.checkOpeness();
  }

}
