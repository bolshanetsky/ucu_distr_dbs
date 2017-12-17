package com.ucu.dist_dbs.lab2;

import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;

/**
 * Created by bolshanetskyi on 17.12.17.
 */
@SuppressWarnings("Duplicates")
public class TransactionsManager {

    public static final String INSERT_FLIGHT_BOOKING = "INSERT INTO public.flights_bookings(\n" + "\tbooking_id, " +
        "client_name, fly_number, \"from\", \"to\", date)\n" + "\tVALUES (?, ?, ?, ?, ?, ?);";

    public static final String INSERT_HOTEL_BOOKING = "INSERT INTO public.hotel_bookings(\n" + "\tbooking_id, " +
            "client_name, hotel_name, arrival, departure)\n" + "\tVALUES (?, ?, ?, ?, ?);";

    private static final String SELECT_CURRENT_ACCOUNT_AMMOUNT = "SELECT ammount\n" + "\tFROM public.account\n"
            + "    WHERE client_name = (?)";

    public static final String UPDATE_ACCOUNT = "UPDATE public.account\n" + "\tSET ammount=?\n" + "\tWHERE " +
            "client_name=?;";
    public static final String FLIGHTS_DATABASE = "flights_booking";
    public static final String HOTELS_DATABASE = "hotel_bookings";
    public static final String ACCOUNTS_DATABASE = "Accounts";
    private HashMap<DB, String> transactions;
    private ConnectionManager connectionManager;
    private boolean isSuccessful = true;

    public void commitTransactions() throws SQLException {
        if (transactions.containsKey(DB.FLIGHTS)) commitFlight();
        if (transactions.containsKey(DB.HOTELS)) commitHotel();
        if (transactions.containsKey(DB.ACCOUNTS)) rollbackAccount();
    }

    public void rollbackTransactions() throws SQLException {
        System.out.println("Rolling back all registered transactions");
        if (transactions.containsKey(DB.FLIGHTS)) rollbackFlight();
        if (transactions.containsKey(DB.HOTELS)) rollbackHotel();
        if (transactions.containsKey(DB.ACCOUNTS)) rollbackAccount();
    }

    public void prepareHotelBooking(Integer bookingId, String clientName, String hotelName,
                                    Date arrival, Date departure) {
        try (Connection connection = connectionManager.getConnection(HOTELS_DATABASE)) {

            String transactionId = RandomStringUtils.randomAlphanumeric(5);

            beginTransaction(connection, transactionId);

            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_HOTEL_BOOKING);
            preparedStatement.setInt(1, bookingId);
            preparedStatement.setString(2, clientName);
            preparedStatement.setString(3, hotelName);
            preparedStatement.setDate(4, arrival);
            preparedStatement.setDate(5, departure);
            preparedStatement.execute();

            transactions.put(DB.HOTELS, transactionId);
            setTransactionId(connection, transactionId);

        } catch (SQLException e) {
            e.printStackTrace();
            this.isSuccessful = false;
        }
    }

    public void prepareFlightBooking(Integer bookingId, String clientName, String
            flightNumber, String from, String to, Date date) throws SQLException {

        try (Connection connection = connectionManager.getConnection(FLIGHTS_DATABASE)) {

            String transactionId = RandomStringUtils.randomAlphanumeric(5);
            beginTransaction(connection, transactionId);

            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_FLIGHT_BOOKING);
            preparedStatement.setInt(1, bookingId);
            preparedStatement.setString(2, clientName);
            preparedStatement.setString(3, flightNumber);
            preparedStatement.setString(4, from);
            preparedStatement.setString(5, to);
            preparedStatement.setDate(6, date);
            preparedStatement.execute();

            transactions.put(DB.FLIGHTS, transactionId);
            setTransactionId(connection, transactionId);

        } catch (SQLException e) {
            e.printStackTrace();
            this.isSuccessful = false;
        }
    }

    public void prepareAccountUpdate(String clientName, BigDecimal cost) throws SQLException {

        try (Connection connection = connectionManager.getConnection(ACCOUNTS_DATABASE)) {

            String transactionId = RandomStringUtils.randomAlphanumeric(5);

            // Get current ammount
            PreparedStatement preparedStatement1 = connection.prepareStatement(SELECT_CURRENT_ACCOUNT_AMMOUNT);
            preparedStatement1.setString(1, clientName);
            ResultSet resultSet = preparedStatement1.executeQuery();
            resultSet.next();
            BigDecimal ammount = resultSet.getBigDecimal(1);

            // Begin transaction
            beginTransaction(connection, transactionId);
            PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_ACCOUNT);
            preparedStatement.setString(2, clientName);
            preparedStatement.setBigDecimal(1, ammount.subtract(cost));
            preparedStatement.executeUpdate();

            transactions.put(DB.ACCOUNTS, transactionId);
            setTransactionId(connection, transactionId);

        } catch (SQLException e) {
            e.printStackTrace();
            this.isSuccessful = false;
        }
    }

    public boolean isPreparedSuccessfuly() {
        return isSuccessful;
    }

    private boolean rollbackHotel() throws SQLException {
        return connectionManager.getConnection(HOTELS_DATABASE).prepareStatement("ROLLBACK PREPARED '" +
                transactions.get(DB.HOTELS) + "'").execute();
    }

    private boolean rollbackFlight() throws SQLException {
        return connectionManager.getConnection(FLIGHTS_DATABASE).prepareStatement("ROLLBACK PREPARED '" +
                transactions.get(DB.FLIGHTS) + "'").execute();
    }

    private boolean rollbackAccount() throws SQLException {
        return connectionManager.getConnection(FLIGHTS_DATABASE).prepareStatement("ROLLBACK PREPARED '" +
                transactions.get(DB.ACCOUNTS) + "'").execute();
    }

    private boolean commitFlight() throws SQLException {
        return connectionManager.getConnection(FLIGHTS_DATABASE).prepareStatement("COMMIT PREPARED '" +
                transactions.get(DB.FLIGHTS) + "'").execute();
    }

    private boolean commitHotel() throws SQLException {
        return connectionManager.getConnection(HOTELS_DATABASE).prepareStatement("COMMIT PREPARED '" + transactions
                .get(DB.HOTELS) + "'").execute();
    }

    private boolean commitAccount() throws SQLException {
        return connectionManager.getConnection(HOTELS_DATABASE).prepareStatement("COMMIT PREPARED '" + transactions
                .get(DB.ACCOUNTS) + "'").execute();
    }

    private boolean setTransactionId(Connection connection, String transactionId) throws SQLException {
        return connection.prepareStatement("PREPARE TRANSACTION '" + transactionId + "';").execute();
    }

    private boolean beginTransaction(Connection connection, String transactionId) throws SQLException {
        return connection.prepareStatement("BEGIN;").execute();
    }

    public TransactionsManager() {
        this.connectionManager = new ConnectionManager();
        this.transactions = new HashMap<>();
    }
}
