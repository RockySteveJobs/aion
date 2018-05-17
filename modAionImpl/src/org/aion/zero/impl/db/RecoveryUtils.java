/*******************************************************************************
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
 * Contributors:
 *     Aion foundation.
 ******************************************************************************/
package org.aion.zero.impl.db;

import org.aion.base.db.IByteArrayKeyValueDatabase;
import org.aion.base.type.IBlock;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.db.impl.DatabaseFactory;
import org.aion.log.AionLoggerFactory;
import org.aion.mcf.config.CfgDb;
import org.aion.mcf.db.IBlockStoreBase;
import org.aion.zero.impl.AionBlockchainImpl;
import org.aion.zero.impl.config.CfgAion;
import org.aion.zero.impl.core.IAionBlockchain;
import org.aion.zero.impl.types.AionBlock;

import java.io.IOException;
import java.util.*;

public class RecoveryUtils {

    public enum Status {
        SUCCESS,
        FAILURE,
        ILLEGAL_ARGUMENT
    }

    /**
     * Used by the CLI call.
     */
    public static Status revertTo(long nbBlock) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "INFO");
        cfgLog.put("GEN", "INFO");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionBlockchainImpl blockchain = AionBlockchainImpl.inst();

        Status status = revertTo(blockchain, nbBlock);

        blockchain.getRepository().close();

        // ok if we managed to get down to the expected block
        return status;
    }

    /**
     * Used by the CLI call.
     */
    public static void pruneAndCorrect() {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        cfg.getDb().setHeapCacheEnabled(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "INFO");
        cfgLog.put("GEN", "INFO");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionBlockchainImpl blockchain = AionBlockchainImpl.inst();

        IBlockStoreBase store = blockchain.getBlockStore();

        IBlock bestBlock = store.getBestBlock();
        if (bestBlock == null) {
            System.out.println("Empty database. Nothing to do.");
            return;
        }

        // revert to block number and flush changes
        store.pruneAndCorrect();
        store.flush();

        blockchain.getRepository().close();
    }

    /**
     * Used by the CLI call.
     */
    public static void dbCompact() {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        cfg.getDb().setHeapCacheEnabled(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "INFO");
        cfgLog.put("GEN", "INFO");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();

        // compact database after the changes were applied
        repository.compact();
        repository.close();
    }

    /**
     * Used by the CLI call.
     */
    public static void dumpBlocks(long count) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        cfg.getDb().setHeapCacheEnabled(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "ERROR");
        cfgLog.put("GEN", "ERROR");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();

        AionBlockStore store = repository.getBlockStore();
        try {
            String file = store.dumpPastBlocks(count, cfg.getBasePath());
            if (file == null) {
                System.out.println("The database is empty. Cannot print block information.");
            } else {
                System.out.println("Block information stored in " + file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        repository.close();
    }

    /**
     * Used by internal world state recovery method.
     */
    public static Status revertTo(IAionBlockchain blockchain, long nbBlock) {
        IBlockStoreBase store = blockchain.getBlockStore();

        IBlock bestBlock = store.getBestBlock();
        if (bestBlock == null) {
            System.out.println("Empty database. Nothing to do.");
            return Status.ILLEGAL_ARGUMENT;
        }

        long nbBestBlock = bestBlock.getNumber();

        System.out.println("Attempting to revert best block from " + nbBestBlock + " to " + nbBlock + " ...");

        // exit with warning if the given block is larger or negative
        if (nbBlock < 0) {
            System.out.println(
                    "Negative values <" + nbBlock + "> cannot be interpreted as block numbers. Nothing to do.");
            return Status.ILLEGAL_ARGUMENT;
        }
        if (nbBestBlock == 0) {
            System.out.println("Only genesis block in database. Nothing to do.");
            return Status.ILLEGAL_ARGUMENT;
        }
        if (nbBlock == nbBestBlock) {
            System.out.println(
                    "The block " + nbBlock + " is the current best block stored in the database. Nothing to do.");
            return Status.ILLEGAL_ARGUMENT;
        }
        if (nbBlock > nbBestBlock) {
            System.out.println("The block #" + nbBlock + " is greater than the current best block #" + nbBestBlock
                                       + " stored in the database. "
                                       + "Cannot move to that block without synchronizing with peers. Start Aion instance to sync.");
            return Status.ILLEGAL_ARGUMENT;
        }

        // revert to block number and flush changes
        store.revert(nbBlock);
        store.flush();

        nbBestBlock = store.getBestBlock().getNumber();

        // ok if we managed to get down to the expected block
        return (nbBestBlock == nbBlock) ? Status.SUCCESS : Status.FAILURE;
    }

    public static void printStateTrieSize(long blockNumber) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "ERROR");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();
        AionBlockStore store = repository.getBlockStore();

        long topBlock = store.getMaxNumber();
        if (topBlock < 0) {
            System.out.println("The database is empty. Cannot print block information.");
            return;
        }

        long targetBlock = topBlock - blockNumber + 1;
        if (targetBlock < 0) {
            targetBlock = 0;
        }

        AionBlock block;
        byte[] stateRoot;

        while (targetBlock <= topBlock) {
            block = store.getChainBlockByNumber(targetBlock);
            if (block != null) {
                stateRoot = block.getStateRoot();
                try {
                    System.out.println(
                            "Block hash: " + block.getShortHash() + ", number: " + block.getNumber() + ", tx count: "
                                    + block.getTransactionsList().size() + ", state trie kv count = " + repository
                                    .getWorldState().getTrieSize(stateRoot));
                } catch (RuntimeException e) {
                    System.out.println(
                            "Block hash: " + block.getShortHash() + ", number: " + block.getNumber() + ", tx count: "
                                    + block.getTransactionsList().size() + ", state trie kv count threw exception: " + e
                                    .getMessage());
                }
            } else {
                System.out.println("Null block found at level " + targetBlock + ".");
            }
            targetBlock++;
        }

        repository.close();
    }

    public static void printStateTrieDump(long blockNumber) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "ERROR");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();

        AionBlockStore store = repository.getBlockStore();

        AionBlock block;

        if (blockNumber == -1L) {
            block = store.getBestBlock();
            if (block == null) {
                System.out.println("The requested block does not exist in the database.");
                return;
            }
            blockNumber = block.getNumber();
        } else {
            block = store.getChainBlockByNumber(blockNumber);
            if (block == null) {
                System.out.println("The requested block does not exist in the database.");
                return;
            }
        }

        byte[] stateRoot = block.getStateRoot();
        System.out.println("\nBlock hash: " + block.getShortHash() + ", number: " + blockNumber + ", tx count: " + block
                .getTransactionsList().size() + "\n\n" + repository.getWorldState().getTrieDump(stateRoot));

        repository.close();
    }

    public static void archiveStates(int current_count, int archive_rate) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "INFO");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();

        AionBlockStore store = repository.getBlockStore();

        System.out.println("Creating temporary database for archiving states.");
        Properties props = cfg.getDb().asProperties().get(CfgDb.Names.DEFAULT);
        props.setProperty(DatabaseFactory.Props.DB_NAME, "temp");
        props.setProperty(DatabaseFactory.Props.DB_PATH, cfg.getDb().getPath());

        IByteArrayKeyValueDatabase archive = DatabaseFactory.connect(props);

        // starting from an empty db
        archive.open();
        archive.drop();

        // check object status
        if (archive == null) {
            System.out.println("Connection could not be established to temporary database.");
        }

        // check persistence status
        if (!archive.isCreatedOnDisk()) {
            System.out.println("Temporary database cannot be saved to disk.");
        }

        long topBlock = store.getMaxNumber();
        long switchBlock = topBlock - current_count;

        // first: archive all the blocks from genesis at given rate
        long currentBlock = 0;
        AionBlock block;
        byte[] stateRoot;

        while (currentBlock < switchBlock) {
            System.out.println("Getting state for " + currentBlock);
            block = store.getChainBlockByNumber(currentBlock);
            ensureNotNull(block);
            stateRoot = block.getStateRoot();
            repository.getWorldState().saveDiffStateToDatabase(stateRoot, archive);
            currentBlock += archive_rate;
        }

        // second: archive the top X blocks
        currentBlock = switchBlock;

        while (currentBlock <= topBlock) {
            System.out.println("Getting state for " + currentBlock);
            block = store.getChainBlockByNumber(currentBlock);
            ensureNotNull(block);
            stateRoot = block.getStateRoot();
            repository.getWorldState().saveDiffStateToDatabase(stateRoot, archive);
            currentBlock++;
        }

        archive.close();
        repository.close();
    }

    private static void ensureNotNull(AionBlock block) {
        if (block == null) {
            System.out.format("Missing main chain block at level %d. Please run < ./aion.sh -r > to correct data.",
                              block);
            System.exit(0);
        }
    }

    public static void archiveState(int blockNumber) {
        // ensure mining is disabled
        CfgAion cfg = CfgAion.inst();
        cfg.dbFromXML();
        cfg.getConsensus().setMining(false);

        Map<String, String> cfgLog = new HashMap<>();
        cfgLog.put("DB", "INFO");

        AionLoggerFactory.init(cfgLog);

        // get the current blockchain
        AionRepositoryImpl repository = AionRepositoryImpl.inst();

        AionBlockStore store = repository.getBlockStore();

        long topBlock = store.getMaxNumber();
        Set<ByteArrayWrapper> usefulKeys = new HashSet<>();
        long targetBlock = blockNumber;
        if (targetBlock < 0) {
            targetBlock = 0;
        }
        if (targetBlock > topBlock) {
            targetBlock = topBlock - 1;
        }

        System.out.println("Creating swap database.");
        Properties props = cfg.getDb().asProperties().get(CfgDb.Names.DEFAULT);
        props.setProperty(DatabaseFactory.Props.DB_NAME, "swap");
        props.setProperty(DatabaseFactory.Props.DB_PATH, cfg.getDb().getPath());

        IByteArrayKeyValueDatabase swapDB = DatabaseFactory.connect(props);

        // open the database connection
        swapDB.open();

        // check object status
        if (swapDB == null) {
            System.out.println("Swap database connection could not be established.");
        }

        // check persistence status
        if (!swapDB.isCreatedOnDisk()) {
            System.out.println("Sawp database cannot be saved to disk.");
        }

        // trace full state for bottom block to swap database
        System.out.println("Getting full state for " + targetBlock);
        AionBlock block = store.getChainBlockByNumber(targetBlock);
        byte[] stateRoot = block.getStateRoot();
        repository.getWorldState().saveFullStateToDatabase(stateRoot, swapDB);

        while (targetBlock < topBlock) {
            targetBlock++;
            System.out.println("Getting diff state for " + targetBlock);
            block = store.getChainBlockByNumber(targetBlock);
            stateRoot = block.getStateRoot();
            repository.getWorldState().saveDiffStateToDatabase(stateRoot, swapDB);
        }

        //        topBlock = blockNumber - 1;
        //        targetBlock = 1;
        //        while (targetBlock <= topBlock) {
        //            System.out.println("Deleting diff state for " + targetBlock);
        //            block = store.getChainBlockByNumber(targetBlock);
        //            stateRoot = block.getStateRoot();
        //            repository.getWorldState().deleteDiffStateToDatabase(stateRoot, swapDB);
        //            targetBlock++;
        //        }

        repository.getWorldState().pruneAllExcept(swapDB);
        swapDB.purge();
        repository.close();
    }
}
