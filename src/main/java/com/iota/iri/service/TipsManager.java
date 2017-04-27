package com.iota.iri.service;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.iota.iri.LedgerValidator;
import com.iota.iri.conf.Configuration;
import com.iota.iri.model.Hash;
import com.iota.iri.controllers.*;
import com.iota.iri.utils.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iota.iri.Milestone;

public class TipsManager {

    private static final Logger log = LoggerFactory.getLogger(TipsManager.class);

    private static int RATING_THRESHOLD = 75; // Must be in [0..100] range

    public static void setRATING_THRESHOLD(int value) {
        if (value < 0) value = 0;
        if (value > 100) value = 100;
        RATING_THRESHOLD = value;
    }
    
    static Hash transactionToApprove(final Hash extraTip, final int depth, Random seed) {

        int milestoneDepth = depth;

        long startTime = System.nanoTime();

        if(Milestone.latestSolidSubtangleMilestoneIndex > Milestone.MILESTONE_START_INDEX ||
                Milestone.latestMilestoneIndex == Milestone.MILESTONE_START_INDEX) {
            final Hash preferableMilestone = Milestone.latestSolidSubtangleMilestone;

            Map<Hash, Long> ratings = new HashMap<>();
            Set<Hash> analyzedTips = new HashSet<>();
            try {
                int traversedTails = 0;
                Hash tip = preferableMilestone;
                if (extraTip != null) {
                    int milestoneIndex = Milestone.latestSolidSubtangleMilestoneIndex - milestoneDepth;
                    if(milestoneIndex < 0) {
                        milestoneIndex = 0;
                    }
                    if(!MilestoneViewModel.load(milestoneIndex)) {
                        Map.Entry<Integer, Hash> closestGreaterMilestone = Milestone.findMilestone(milestoneIndex);
                        new MilestoneViewModel(closestGreaterMilestone.getKey(), closestGreaterMilestone.getValue()).store();
                        tip = closestGreaterMilestone.getValue();
                    } else {
                        tip = MilestoneViewModel.get(milestoneIndex).getHash();
                    }
                }
                Hash tail = tip;

                updateRatings(tip, ratings, analyzedTips);
                analyzedTips.clear();

                String traversedPaths = "Tail: " + tail.toString() + " Rating: " + ratings.get(tip);
                Hash[] tips;
                TransactionViewModel transactionViewModel;
                int carlo;
                double monte;
                while (tip != null) {
                    tips = TransactionViewModel.fromHash(tip).getApprovers();
                    if (tips.length == 0) {
                        break;
                    }
                    if (!ratings.containsKey(tip)) {
                        updateRatings(tip, ratings, analyzedTips);
                        analyzedTips.clear();
                    }
                    //traversedPaths += " Ratings: " + ratings.values().stream().map(l -> String.valueOf(l) + " / ").reduce(String::concat).orElse("/");
                    monte = seed.nextDouble() * ratings.get(tip);
                    for (carlo = tips.length; carlo-- > 1; ) {
                        if (ratings.containsKey(tips[carlo])) {
                            monte -= ratings.get(tips[carlo]);
                        }
                        if (monte <= 0) {
                            break;
                        }
                    }
                    traversedPaths += "\nRandom selection index: " + carlo + " Hash:" + tips[carlo].toString();
                    transactionViewModel = TransactionViewModel.fromHash(tips[carlo]);
                    if (transactionViewModel == null) {
                        log.info("Tip not found. ");
                        break;
                    } else if (!TransactionRequester.instance().checkSolidity(transactionViewModel.getHash(), false)) {
                        log.info("Transaction not solid.");
                        break;
                    } else if (!LedgerValidator.updateFromSnapshot(transactionViewModel.getHash())) {
                        log.info("Bundle not consistent.");
                        break;
                    } else if (transactionViewModel.getHash().equals(extraTip) || transactionViewModel.getHash().equals(tip)) {
                        break;
                    } else {
                        traversedTails++;
                        tip = transactionViewModel.getHash();
                        if(transactionViewModel.getCurrentIndex() == 0) {
                            tail = tip;
                            traversedPaths += "\nTail: " + tail.toString() + " Rating: " + ratings.get(tip);
                        } else {
                            //traversedPaths += "\nTip: " + tip.toString() + " Rating: " + ratings.get(tip);
                        }
                    }
                }
                log.info("Traversed Path: {}", traversedPaths);
                log.info("Tx traversed to find tip: {}", traversedTails);
                return tail;
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Encountered error: " + e.getLocalizedMessage());
            } finally {
                API.incEllapsedTime_getTxToApprove(System.nanoTime() - startTime);
            }
        }
        return null;
    }

    static long capSum(long a, long b, long max) {
        if(a + b < 0 || a + b > max) {
            return max;
        }
        return a+b;
    }

    private static long updateRatings(Hash txHash, Map<Hash, Long> ratings, Set<Hash> analyzedTips) throws Exception {
        long rating = 1;
        if(analyzedTips.add(txHash)) {
            TransactionViewModel transactionViewModel = TransactionViewModel.fromHash(txHash);
            for(Hash approver : transactionViewModel.getApprovers()) {
                rating = capSum(rating, updateRatings(approver, ratings, analyzedTips), Long.MAX_VALUE/2);
            }
            ratings.put(txHash, rating);
        } else {
            if(ratings.containsKey(txHash)) {
                rating = ratings.get(txHash);
            } else {
                rating = 0;
            }
        }
        return rating;       
    }

    public static TipsManager instance() {
        return instance;
    }
    
    private TipsManager() {}
    
    private static final TipsManager instance = new TipsManager();
}
