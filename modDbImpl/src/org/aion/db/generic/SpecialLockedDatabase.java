/* ******************************************************************************
 * Copyright (c) 2017-2018 Aion foundation.
 *
 *     This file is part of the aion network project.
 *
 *     The aion network project is free software: you can redistribute it
 *     and/or modify it under the terms of the GNU General Public License
 *     as published by the Free Software Foundation, either version 3 of
 *     the License, or any later version.
 *
 *     The aion network project is distributed in the hope that it will
 *     be useful, but WITHOUT ANY WARRANTY; without even the implied
 *     warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *     See the GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with the aion network project source files.
 *     If not, see <https://www.gnu.org/licenses/>.
 *
 *     The aion network project leverages useful source code from other
 *     open source projects. We greatly appreciate the effort that was
 *     invested in these projects and we thank the individual contributors
 *     for their work. For provenance information and contributors
 *     please see <https://github.com/aionnetwork/aion/wiki/Contributors>.
 *
 * Contributors to the aion source files in decreasing order of code volume:
 *     Aion foundation.
 ******************************************************************************/
package org.aion.db.generic;

import java.util.Collection;
import java.util.Map;
import org.aion.base.db.IByteArrayKeyValueDatabase;
import org.aion.base.db.IByteArrayKeyValueStore;

/**
 * Implements locking functionality for a database that is mostly thread-safe except for open and
 * close (like LevelDB).
 *
 * @author Alexandra Roatis
 */
public class SpecialLockedDatabase extends LockedDatabase implements IByteArrayKeyValueDatabase {

    public SpecialLockedDatabase(IByteArrayKeyValueDatabase _unlockedDatabase) {
        super(_unlockedDatabase);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        // acquire read lock
        lock.readLock().lock();

        try {
            database.put(key, value);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not put key-value pair due to ", e);
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(byte[] key) {
        // acquire read lock
        lock.readLock().lock();

        try {
            database.delete(key);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not delete key due to ", e);
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }

    @Override
    public void putBatch(Map<byte[], byte[]> keyValuePairs) {
        // acquire read lock
        lock.readLock().lock();

        try {
            database.putBatch(keyValuePairs);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not put batch due to ", e);
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }

    @Override
    public void deleteBatch(Collection<byte[]> keys) {
        // acquire read lock
        lock.readLock().lock();

        try {
            database.deleteBatch(keys);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not delete batch due to ", e);
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }

    @Override
    public long deleteAllExcept(IByteArrayKeyValueStore db) {
        // acquire read lock
        lock.readLock().lock();

        try {
            return database.deleteAllExcept(db);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not delete keys from database due to ", e);
                return -1L;
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }

    @Override
    public long deleteAllExcept(IByteArrayKeyValueStore db, long limit) {
        // acquire read lock
        lock.readLock().lock();

        try {
            return database.deleteAllExcept(db, limit);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw e;
            } else {
                LOG.error("Could not delete keys from database due to ", e);
                return -1L;
            }
        } finally {
            // releasing read lock
            lock.readLock().unlock();
        }
    }
}
