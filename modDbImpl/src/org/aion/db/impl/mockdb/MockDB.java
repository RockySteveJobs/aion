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
package org.aion.db.impl.mockdb;

import java.util.*;
import org.aion.base.db.IByteArrayKeyValueStore;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.db.impl.AbstractDB;

public class MockDB extends AbstractDB {

    private Map<ByteArrayWrapper, byte[]> kv;

    public MockDB(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":<name=" + name + ">";
    }

    // IDatabase functionality
    // -----------------------------------------------------------------------------------------

    @Override
    public boolean open() {
        if (isOpen()) {
            return true;
        }

        LOG.debug("init database {}", this.toString());

        // using a regular map since synchronization is handled through the read-write lock
        kv = new HashMap<>();

        return isOpen();
    }

    @Override
    public void close() {
        // release resources if needed
        if (kv != null) {
            LOG.info("Closing database " + this.toString());

            kv.clear();
        }

        // set map to null
        kv = null;
    }

    @Override
    public boolean isOpen() {
        return kv != null;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isCreatedOnDisk() {
        return false;
    }

    @Override
    public long approximateSize() {
        check();
        return -1L;
    }

    // IKeyValueStore functionality
    // ------------------------------------------------------------------------------------

    @Override
    public boolean isEmpty() {
        check();
        return kv.isEmpty();
    }

    @Override
    public Set<byte[]> keys() {
        Set<byte[]> set = new HashSet<>();

        check();

        kv.keySet().forEach(k -> set.add(k.getData()));

        // empty when retrieval failed
        return set;
    }

    @Override
    public byte[] getInternal(byte[] k) {
        return kv.get(ByteArrayWrapper.wrap(k));
    }

    @Override
    public void put(byte[] k, byte[] v) {
        check(k);
        check();

        if (v == null) {
            kv.remove(ByteArrayWrapper.wrap(k));
        } else {
            kv.put(ByteArrayWrapper.wrap(k), v);
        }
    }

    @Override
    public void delete(byte[] k) {
        check(k);
        check();

        kv.remove(ByteArrayWrapper.wrap(k));
    }

    @Override
    public void putBatch(Map<byte[], byte[]> inputMap) {
        check(inputMap.keySet());

        // this runtime exception should not be caught here
        check();

        try {
            // simply do a put, because setting a kv pair to null is same as delete
            inputMap.forEach(
                    (key, value) -> {
                        if (value == null) {
                            kv.remove(ByteArrayWrapper.wrap(key));
                        } else {
                            kv.put(ByteArrayWrapper.wrap(key), value);
                        }
                    });
        } catch (Exception e) {
            LOG.error(
                    "Unable to execute batch put/update operation on " + this.toString() + ".", e);
        }
    }

    @Override
    public void putToBatch(byte[] k, byte[] v) {
        // same as put since batch operations are not supported
        put(k, v);
    }

    @Override
    public void commitBatch() {
        // nothing to do since batch operations are not supported
    }

    @Override
    public void deleteBatch(Collection<byte[]> keys) {
        check(keys);

        // this runtime exception should not be caught here
        check();

        try {
            keys.forEach((e) -> kv.remove(ByteArrayWrapper.wrap(e)));
        } catch (Exception e) {
            LOG.error("Unable to execute batch delete operation on " + this.toString() + ".", e);
        }
    }

    @Override
    public void drop() {
        kv.clear();
    }

    @Override
    public long deleteAllExcept(IByteArrayKeyValueStore _db) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Deleting all keys from {} that are not in {}.",
                    this.toString(),
                    _db.toString());
        }

        long delCount = 0;

        try {
            Iterator<ByteArrayWrapper> iter = kv.keySet().iterator();

            // extract keys
            while (iter.hasNext()) {
                ByteArrayWrapper key = iter.next();
                if (!_db.get(key.getData()).isPresent()) {
                    kv.remove(key);
                    delCount++;
                }
            }
        } catch (Exception e) {
            LOG.error(
                    "Unable to delete all keys from "
                            + this.toString()
                            + " that are not in "
                            + _db.toString()
                            + " due to: ",
                    e);
        }

        return delCount;
    }

    @Override
    public long deleteAllExcept(IByteArrayKeyValueStore _db, long limit) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Deleting all keys from {} that are not in {} with explore limit {}.",
                    this.toString(),
                    _db.toString(),
                    limit);
        }

        long delCount = 0, viewCount = 0;

        try {
            Iterator<ByteArrayWrapper> iter = kv.keySet().iterator();

            // extract keys
            while (iter.hasNext() && viewCount <= limit) {
                ByteArrayWrapper key = iter.next();
                if (!_db.get(key.getData()).isPresent()) {
                    kv.remove(key);
                    delCount++;
                }
                viewCount++;
            }
        } catch (Exception e) {
            LOG.error(
                    "Unable to delete all keys from "
                            + this.toString()
                            + " that are not in "
                            + _db.toString()
                            + " with explore limit "
                            + limit
                            + " due to: ",
                    e);
        }

        return delCount;
    }
    // AbstractDB functionality
    // ----------------------------------------------------------------------------------------

    public boolean commitCache(Map<ByteArrayWrapper, byte[]> cache) {
        boolean success = false;

        try {
            check();

            // simply do a put, because setting a kv pair to null is same as delete
            cache.forEach(
                    (key, value) -> {
                        if (value == null) {
                            kv.remove(key);
                        } else {
                            kv.put(key, value);
                        }
                    });

            success = true;
        } catch (Exception e) {
            LOG.error("Unable to commit heap cache to " + this.toString() + ".", e);
        }

        return success;
    }
}
