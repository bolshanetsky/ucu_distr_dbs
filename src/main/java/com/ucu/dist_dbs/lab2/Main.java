package com.ucu.dist_dbs.lab2;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Created by bolshanetskyi on 17.12.17.
 */
public class Main {

    public static void main(String[] args) throws SQLException {

        Random random = new Random();

        TransactionsManager transactionManager = new TransactionsManager();

        try {

            // Preparing transaction
            transactionManager.prepareFlightBooking(random.nextInt(Integer.MAX_VALUE), RandomStringUtils.randomAlphanumeric(8), RandomStringUtils.randomAlphabetic(4), "Rome", "Lviv", new Date(System.currentTimeMillis()));

            if (transactionManager.isPreparedSuccessfuly()) {
                transactionManager.prepareHotelBooking(random.nextInt(Integer.MAX_VALUE), RandomStringUtils.randomAlphanumeric(8), RandomStringUtils.randomAlphabetic(4), new Date(System.currentTimeMillis()), new Date(System.currentTimeMillis()));
            }

            if (transactionManager.isPreparedSuccessfuly()) {
                transactionManager.prepareAccountUpdate("Vasya", new BigDecimal(250));
            }

            // Commit if prepare successful or rollback
            if (transactionManager.isPreparedSuccessfuly()) {
                System.out.println("Transaction was prepared successfully. Commit.....");
                transactionManager.commitTransactions();
            } else {
                // Rollback if not successful
                System.out.println("Transaction wasn't successful. Rollback.....");
                transactionManager.rollbackTransactions();
            }
        }

        catch (SQLException ex) {
            ex.printStackTrace();
            // try to rollback in any case
            transactionManager.rollbackTransactions();
        }
    }

}
