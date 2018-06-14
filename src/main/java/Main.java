import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {
    private final static String PROPERTY_PATH = "src/main/resources/DBProp.properties"; //Путь к файлу properties
    //КЛЮЧИ для файла properties
    private final static String ADMIN_PASSWORD = "admin_password";  //для получения пароля администратора
    private final static String JDBC_DRIVER = "JDBC_driver";        //для получения JDBC драйвера
    private final static String DBMS_PASSWORD = "DBMS_password";    //для получения пароля доступа к СУБД
    private final static String DBMS_LOGIN = "DBMS_login";          //для получения логина доступа к СУБД
    private final static String DB_URL = "DB_URL";                  //для получения URL базы данных
    private final static String USERS_TABLE = "users_table";        //для получения названия таблицы пользователей
    private final static String TOPICS_TABLE = "topics_table";      //для получения названия таблицы категорий вопросов
    private final static String QUESTIONS_TABLE = "questions_table";//для получения названия таблицы вопросов
    private final static String PROBABLE_ANSWERS_TABLE = "probable_answers_table";//для получения названия таблицы возможных ответов
    private final static String ATTEMPS_TABLE = "attempts_table";   //для получения названия таблицы попыток сдачи
    private final static String LOGIN_FIELD = "login_field";        //для получения названия поля с логинами из таблицы пользователей
    private final static String PASSWORD_FIELD = "password_field";  //для получения названия поля с паролями из таблицы пользователей
    private final static String NAME_FIELD = "name_field";          //для получения названия поля с именем из таблицы пользователей
    private final static String TOPIC_FIELD = "topic_field";        //для получения названия поля с названиями темы из таблицы тем
    private final static String TOPIC_ID_FIELD = "topic_id_field";  //для получения названия поля с id темы из таблицы тем
    private final static String QUESTION_ID_FIELD = "question_id_field";//для получения названия поля с id вопроса из таблицы вопросов
    private final static String QUESTION_FIELD = "question_field";  //для получения названия поля с вопросом из таблицы вопросов
    private final static String ANSWER_FIELD = "answer_field";      //для получения названия поля с ответами из таблицы ответов
    private final static String IS_CORRECT_FIELD = "is_correct_field";//для получения названия поля с метками "верно-неверно" из таблицы ответов
    private final static String DATE_FIELD = "date_field";          //для получения названия поля с датой прохождения теста
    private final static String RESULT_FIELD = "result_field";      //для получения названия поля с результатом теста
    private final static String ATTEMPT_ID_FIELD = "attempt_id_field";//для получения названия поля с id попытки прохождения теста
    private final static String GIVEN_ANSWER_FIELD = "given_answer_field";//для получения названия поля ответа, данного пользователем
    private final static String QUESTIONS_TO_ASK_COUNT = "questions_to_ask_count";  //для получения количества вопросов, включаемых в тест, из каждой категории

    public static void main(String[] args) {
        final int EXIT = 0;
        final int LOGIN = 1;
        final int REGISTER = 2;
        final int EDIT = 3;
        try (InputStream in = new FileInputStream(PROPERTY_PATH)) {
            Properties properties = new Properties();
            properties.loadFromXML(in);  //Загружаем properties, в котором хранится много чего нужного
            Class.forName((String) properties.get(JDBC_DRIVER));  //Подгружаем Driver JDBC
            int choice;
            do {
                Service.cleanConsole();
                System.out.println("MENU");
                System.out.println(LOGIN + " - LOGIN");
                System.out.println(REGISTER + " - REGISTER");
                System.out.println(EDIT + " - EDIT");
                System.out.println(EXIT + " - EXIT");
                System.out.print("Your choice:");
                choice = Service.getIntegerBounded(EXIT, EDIT);
                if (choice != EXIT) {
                    String login = (String) properties.get(DBMS_LOGIN);  //Загружаем логин для доступа к СУБД
                    String password = (String) properties.get(DBMS_PASSWORD);    //Пароль для доступа к СУБД
                    String url = (String) properties.get(DB_URL);                //URL базы данных
                    try (Connection connection = DriverManager.getConnection(url, login, password)) {
                        switch (choice) {
                            case LOGIN: {
                                userLoginProcess(connection, properties);
                            }
                            break;
                            case REGISTER: {
                                registerProcess(connection, properties);
                            }
                            break;
                            case EDIT: {
                                adminLoginProcess(connection, properties);
                            }
                            break;
                        }
                    } catch (SQLException e) {
                        System.out.println("SQL-error has happened. Program will be shutdown.");
                        break;
                    }
                }
            } while (choice != EXIT);
            System.out.println("Buy-buy!");
        } catch (ClassNotFoundException e) {
            System.out.println("Fatal error! SQL-driver was not found.");
            Service.pressEnterToContinue();
        } catch (FileNotFoundException e) {
            System.out.println("Fatal error! Database properties-file was not found.");
            Service.pressEnterToContinue();
        } catch (IOException e) {
            System.out.println("Fatal error! IOException has happened.");
            Service.pressEnterToContinue();
        }
    }

    //Процедура логирования пользователя
    private static void userLoginProcess(Connection connection, Properties properties) throws SQLException {
        Service.cleanConsole();
        final String EXIT = "EXIT";
        Scanner scanner = new Scanner(System.in);
        String login;
        String password;
        do {
            System.out.print("Login (EXIT - for exit):");
            login = scanner.nextLine();  //Логин
            if (!login.equals(EXIT)) {
                System.out.print("Password:");
                password = scanner.nextLine();  //Пароль, введенный пользователем
                Statement statement = connection.createStatement();
                ResultSet resultSet = selectWithRestraint(statement, (String) properties.get(PASSWORD_FIELD),
                        (String) properties.get(USERS_TABLE),
                        (String) properties.get(LOGIN_FIELD), login);

                //Если в таблице пользователей нет пользователя с указаынм логином или пароль не соответсвует логину
                if (!resultSet.next() || !resultSet.getString((String) properties.get(PASSWORD_FIELD)).equals(password)) {
                    System.out.println("Access denied!");
                } else {
                    chooseOperationProcess(login, connection, properties);
                    break;
                }
                statement.close();
            }
        } while (!login.equals(EXIT));
    }

    //Процедура регистрации пользователя
    private static void registerProcess(Connection connection, Properties properties) throws SQLException {
        Service.cleanConsole();
        final String EXIT = "EXIT";
        Scanner scanner = new Scanner(System.in);
        String login;
        do {
            Statement statement = connection.createStatement();
            //Список всех логинов из базы данных
            ResultSet logins = select(statement,
                    (String) properties.get(LOGIN_FIELD),
                    (String) properties.get(USERS_TABLE));
            System.out.print("Login (EXIT - for exit):");
            login = scanner.nextLine();  //Логин, введенный пользователем
            if (!login.equals(EXIT)) {
                //Проверяем существует ли уже введенный логин
                boolean newLogin = true;
                while (logins.next()) {
                    if (logins.getString((String) properties.get(LOGIN_FIELD)).equals(login)) {
                        newLogin = false;
                        break;
                    }
                }
                //Если пользователь ввел новый логин
                if (newLogin) {
                    System.out.print("Password:");
                    String password = scanner.nextLine();  //Пароль, введенный пользователем
                    System.out.print("Real name:");
                    String name = scanner.nextLine();  //Имя пользователя
                    insert(connection, (String) properties.get(USERS_TABLE), login, password, name);
                    System.out.println("Registration is complete.");
                    Service.pressEnterToContinue();
                    chooseOperationProcess(login, connection, properties);
                } else {
                    System.out.println("This login is already taken. Try another.");
                }
            }
            statement.close();
        } while (!login.equals(EXIT));
    }

    //Процедура логирования админа
    private static void adminLoginProcess(Connection connection, Properties properties) throws SQLException {
        Service.cleanConsole();
        final String EXIT = "EXIT";
        final String password = (String) properties.get(ADMIN_PASSWORD);   //Актуальный пароль
        Scanner scanner = new Scanner(System.in);
        String line;
        do {
            System.out.print("Password:");
            line = scanner.nextLine();  //Пароль, введенный пользователем
            if (line.equals(password)) {
                editProcess(connection, properties);
                line = EXIT;
            } else if (!line.equals(EXIT)) {
                System.out.println("Access denied!");
            }
        } while (!line.equals(EXIT));
    }

    //Процедура редактирования тестовой базы
    private static void editProcess(Connection connection, Properties properties) throws SQLException {
        final String EXIT = "EXIT";
        final int EXIT_MODE = 0;
        final int ADD_QUESTION = 1;
        final int CREATE_TOPIC = 2;
        int choice;
        do {
            Service.cleanConsole();
            System.out.println("MENU");
            System.out.println(ADD_QUESTION + " - Add question");
            System.out.println(CREATE_TOPIC + " - Create new topic");
            System.out.println(EXIT_MODE + " - Exit");
            System.out.print("Your choice:");
            choice = Service.getIntegerBounded(EXIT_MODE, CREATE_TOPIC);
            switch (choice) {
                case ADD_QUESTION: {
                    addQuestionProcess(connection, properties);
                }
                break;
                case CREATE_TOPIC: {
                    System.out.print("Topic name (EXIT - for exit):");
                    Scanner scanner = new Scanner(System.in);
                    String topic = scanner.nextLine();
                    if (!topic.equals(EXIT)) {
                        createTopic(connection, properties, topic);
                    }
                }
                break;
            }
            if (choice != EXIT_MODE) {
                Service.pressEnterToContinue();
            }
        } while (choice != EXIT_MODE);
    }

    private static void addQuestionProcess(Connection connection, Properties properties) throws SQLException {
        final int EXIT_MODE = 0;
        final int ADD_MODE = 1;
        ArrayList<Integer> topicIds = new ArrayList<>();
        ArrayList<String> topics = new ArrayList<>();
        Statement statement = connection.createStatement();
        ResultSet resultSet = select(statement, "*", (String) properties.get(TOPICS_TABLE));
        while (resultSet.next()) {
            topicIds.add(resultSet.getInt(1));
            topics.add(resultSet.getString(2));
        }
        int choice;
        do {
            Service.cleanConsole();
            System.out.println("SELECT TOPIC");
            for (int i = 0; i < topicIds.size(); i++) {
                System.out.println((i + 1) + " - " + topics.get(i));
            }
            System.out.println(EXIT_MODE + " - Exit");
            System.out.print("Your choice:");
            choice = Service.getIntegerBounded(EXIT_MODE, topicIds.size());
            if (choice != EXIT_MODE) {
                int secondChoice;
                do {
                    Service.cleanConsole();
                    System.out.println("SELECT OPERATION");
                    System.out.println(ADD_MODE + " - Add new question");
                    System.out.println(EXIT_MODE + " - Exit");
                    System.out.print("Your choice:");
                    secondChoice = Service.getIntegerBounded(EXIT_MODE, ADD_MODE);
                    if (secondChoice == ADD_MODE) {
                        System.out.print("Question:");
                        Scanner scanner = new Scanner(System.in);
                        String question = scanner.nextLine();
                        int topicId = topicIds.get(choice - 1);
                        int questionID = insertQuestion(connection, properties, question, topicId);
                        int thirdChoice;
                        int answersCount = 0;
                        final int MIN_ANSWERS = 2;  //Минимальное количество возможных ответов на тест
                        do {
                            Service.cleanConsole();
                            System.out.println("SELECT OPERATION");
                            System.out.println(ADD_MODE + " - Add new answer");
                            System.out.println(EXIT_MODE + " - Exit");
                            System.out.print("Your choice:");
                            thirdChoice = Service.getIntegerBounded(EXIT_MODE, ADD_MODE);
                            switch (thirdChoice) {
                                case ADD_MODE: {
                                    System.out.print("Answer:");
                                    String answer = scanner.nextLine();
                                    System.out.print("Is it correct (1/0):");
                                    boolean isCorrect = (Service.getIntegerBounded(0,1) == 1);
                                    insertAnswer(connection, properties, answer, isCorrect, questionID);
                                    answersCount++;
                                }
                                break;
                                case EXIT_MODE: {
                                    if (answersCount < MIN_ANSWERS) {
                                        System.out.println("Minimum answers count is " + MIN_ANSWERS + ".");
                                        System.out.println("Please, add more answers.");
                                    }
                                }
                                break;
                            }
                        } while (thirdChoice != EXIT_MODE || answersCount < MIN_ANSWERS);
                    }
                } while (secondChoice != EXIT_MODE);
            }
        } while (choice != EXIT_MODE);
    }

    private static int insertQuestion(Connection connection, Properties properties, String question, int topicId) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("INSERT INTO " + properties.get(QUESTIONS_TABLE) + " (" +
                properties.get(QUESTION_FIELD) + ", " +
                properties.get(TOPIC_ID_FIELD) + ") VALUES ('" + question + "', " + topicId + ") RETURNING " +
                properties.get(QUESTION_ID_FIELD));
        ResultSet resultSet = statement.getResultSet();
        int id;
        if (resultSet.next()) {
            id = resultSet.getInt((String) properties.get(QUESTION_ID_FIELD));
            statement.close();
            return id;
        } else {
            throw new SQLException("Something bad has happened while INSERT operation");
        }
    }

    private static void insertAnswer(Connection connection, Properties properties, String answer, boolean isCorrect, int questionId) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("INSERT INTO " + properties.get(PROBABLE_ANSWERS_TABLE) + " (" +
                properties.get(ANSWER_FIELD) + ", " +
                properties.get(IS_CORRECT_FIELD) + ", " +
                properties.get(QUESTION_ID_FIELD) + ") VALUES ('" + answer + "', " + isCorrect + ", " + questionId + ")");
    }

    private static void createTopic(Connection connection, Properties properties, String topic) throws SQLException {
//        //Не понимаю почему в этом коде вылетает SQLException. При этом с preparedStatement проблем не возникает.
//        Statement statement = connection.createStatement();
//        statement.executeUpdate("INSERT INTO " + properties.get(TOPICS_TABLE) + " (" +
//                properties.get(TOPIC_FIELD) + ") VALUES (" +
//                properties.get(TOPIC_FIELD) + " = '" + topic + "')");
//        System.out.println("New topic was created.");
//        statement.close();

        String line = "INSERT INTO " + properties.get(TOPICS_TABLE) + " (" + properties.get(TOPIC_FIELD) + ") VALUES (?)";
        PreparedStatement preparedStatement = connection.prepareStatement(line);
        preparedStatement.setString(1, topic);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private static ResultSet selectWithRestraint(Statement statement, String column, String table, String restraint, String value) throws SQLException {
        return statement.executeQuery("SELECT " + column + " FROM " + table + " WHERE " + restraint + " = '" + value + "'");
    }

    private static ResultSet selectWithIntRestraint(Statement statement, String column, String table, String restraint, int value) throws SQLException {
        return statement.executeQuery("SELECT " + column + " FROM " + table + " WHERE " + restraint + " = " + value);
    }

    private static ResultSet select(Statement statement, String column, String table) throws SQLException {
        return statement.executeQuery("SELECT " + column + " FROM " + table);
    }

    //Метод вставки строки в таблицу, в которой все поля текстовые
    private static void insert(Connection connection, String table, String... fields) throws SQLException {
        Statement statement = connection.createStatement();
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(table);
        query.append(" VALUES (");
        for (int i = 0; i < fields.length; i++) {
            query.append("'");
            query.append(fields[i]);
            query.append("'");
            if (i < fields.length - 1) {
                query.append(",");
            }
        }
        query.append(")");
        statement.executeUpdate(new String(query));
        statement.close();
    }

    private static int insertAttempt(Connection connection, Properties properties, String table, String login, double result) throws SQLException {
        Statement statement = connection.createStatement();
        //Почему-то если использовать метод executeUpdate(), вылетает SQLException. Причем вылетает только если
        //statement добавить RETURNING attempt_id
        statement.execute("INSERT INTO " + table + " (" +
                properties.get(LOGIN_FIELD) + ", " +
                properties.get(DATE_FIELD) + ", " +
                properties.get(RESULT_FIELD) + ") VALUES ('" +
                login + "', current_timestamp, " + result + ") RETURNING " +
                properties.get(ATTEMPT_ID_FIELD));
        ResultSet resultSet = statement.getResultSet();
        int id;
        if (resultSet.next()) {
            id = resultSet.getInt((String) properties.get(ATTEMPT_ID_FIELD));
            statement.close();
            return id;
        } else {
            throw new SQLException("Something bad has happened while INSERT operation");
        }
    }

    private static int insertAttemptPrep(Connection connection, Properties properties, String table, String login, double result) throws SQLException {
        String line = "INSERT INTO " + table + " (" +
                properties.get(LOGIN_FIELD) + ", " +
                properties.get(DATE_FIELD) + ", " +
                properties.get(RESULT_FIELD) + ") VALUES (?,?,?) RETURNING " +
                properties.get(ATTEMPT_ID_FIELD);
        System.out.println(line);
        PreparedStatement statement = connection.prepareStatement(line);
        statement.setString(1, login);
        Timestamp time = new Timestamp(System.currentTimeMillis());
        statement.setTimestamp(2, time);
        statement.setDouble(3, result);

        statement.execute();    //Почему-то если использовать метод executeUpdate(), вылетает SQLException. Причем вылетает только если
        //statement добавить RETURNING attempt_id

        ResultSet resultSet = statement.getResultSet();
        int id;
        if (resultSet.next()) {
            id = resultSet.getInt((String) properties.get(ATTEMPT_ID_FIELD));
            statement.close();
            return id;
        } else {
            throw new SQLException("Something bad has happened while INSERT operation");
        }
    }

    //Процедура выбор действия пользователем
    private static void chooseOperationProcess(String login, Connection connection, Properties properties) throws SQLException {
        final int TRY_TEST = 1;
        final int SHOW_HISTORY = 2;
        final int EXIT = 0;
        int choice;
        do {
            Service.cleanConsole();
            System.out.println("MENU");
            System.out.println(TRY_TEST + " - Try test");
            System.out.println(SHOW_HISTORY + " - Show history");
            System.out.println(EXIT + " - Exit");
            System.out.print("Your choice:");
            choice = Service.getIntegerBounded(EXIT, SHOW_HISTORY);
            switch (choice) {
                case TRY_TEST: {
                    tryTest(login, connection, properties);
                }
                break;
                case SHOW_HISTORY: {
                    showHistory(login, connection, properties);
                }
                break;
            }
        } while (choice != EXIT);
    }

    //Процедура отображения истории прохождения тестов
    private static void showHistory(String login, Connection connection, Properties properties) throws SQLException {
        final String EXIT = "EXIT";
        final int EXIT_MODE = 0;
        final int LAST_MODE = 1;
        final int ALL_MODE = 2;
        int choice;
        do {
            Service.cleanConsole();
            System.out.println(LAST_MODE + " - Show last attempt");
            System.out.println(ALL_MODE + " - Show all attempts");
            System.out.println(EXIT_MODE + " - Exit");
            System.out.print("Your choice:");
            choice = Service.getIntegerBounded(EXIT_MODE, ALL_MODE);
            switch (choice) {
                case LAST_MODE: {
                    //Покажем детальную картину последнего прохождения теста
                    //Выберем общую информацию о попытках сдачи (время и результат)
                    Statement statement = connection.createStatement();
                    statement.executeQuery("SELECT * FROM " + properties.get(ATTEMPS_TABLE) +
                            " WHERE " + properties.get(LOGIN_FIELD) + " = '" + login + "'");
                    ResultSet attemptsResultSet = statement.getResultSet();
                    int id = 0;
                    Timestamp time = null;
                    double result = 0;
                    while (attemptsResultSet.next()) {
                        id = attemptsResultSet.getInt(1);
                        time = attemptsResultSet.getTimestamp(3);
                        result = attemptsResultSet.getDouble(4);
                    }

                    //Общая информация по последнему тесту
                    System.out.println("===========================GENERAL INFORMATION==================================");
                    System.out.println("attempt id:" + id);
                    System.out.println("date: " + time);
                    Formatter formatter = new Formatter();
                    formatter.format("result: %.2f%%", result);
                    System.out.println(formatter);
                    formatter.close();

                    //Покажем детали прохождения выбраного теста
                    System.out.println("===========================DETAILS==============================================");
                    Statement answersStatement = connection.createStatement();
                    answersStatement.executeQuery("SELECT * FROM given_answers WHERE attempt_id = " + id);
                    ResultSet answersResultSet = answersStatement.getResultSet();
                    int i = 1;
                    while (answersResultSet.next()) {
                        System.out.println("question # " + i++);
                        System.out.println("Your answer: " + answersResultSet.getString(3));
                        if (answersResultSet.getBoolean(4)) {
                            System.out.println("IS CORRECT");
                        } else {
                            System.out.println("FALSE");
                        }
                        System.out.println("--------------------------------------------------------------------------------");
                    }
                }
                break;
                case ALL_MODE: {
                    //Выберем общую информацию о попытках сдачи (время и результат)
                    Statement statement = connection.createStatement();
                    statement.executeQuery("SELECT * FROM " + properties.get(ATTEMPS_TABLE) +
                            " WHERE " + properties.get(LOGIN_FIELD) + " = '" + login + "'");
                    ResultSet attemptsResultSet = statement.getResultSet();
                    while (attemptsResultSet.next()) {
                        System.out.println("============================================================================");
                        System.out.println("attempt id:" + attemptsResultSet.getInt(1));
                        System.out.println("date: " + attemptsResultSet.getTimestamp(3));
                        Formatter formatter = new Formatter();
                        formatter.format("result: %.2f%%", attemptsResultSet.getDouble(4));
                        System.out.println(formatter);
                        formatter.close();
                    }
                    statement.close();
                    System.out.print("Specify attempt id to see details (or EXIT - for exit):");
                    Scanner scanner = new Scanner(System.in);
                    String reply = scanner.nextLine();
                    //Если пользователь хочет увидеть детали прохождения одного из тестов
                    if (!reply.equals(EXIT)) {
                        try {
                            int id = Integer.parseInt(reply);
                            //Проверим правильно ли пользоватеь указал id попытки (чтобы он не смог результат другого учасника)
                            Statement checkStatement = connection.createStatement();
                            checkStatement.executeQuery("SELECT * FROM " + properties.get(ATTEMPS_TABLE) +
                                    " WHERE " + properties.get(ATTEMPT_ID_FIELD) + " = " + id);
                            ResultSet checkResultSet = checkStatement.getResultSet();
                            if (checkResultSet.next()) {
                                String checkedLogin = checkResultSet.getString(2);
                                double result = checkResultSet.getDouble(4);
                                if (login.equals(checkedLogin)) {
                                    //Покажем детали прохождения выбраного теста
                                    Statement answersStatement = connection.createStatement();
                                    answersStatement.executeQuery("SELECT * FROM given_answers WHERE attempt_id = " + id);
                                    ResultSet answersResultSet = answersStatement.getResultSet();
                                    int i = 1;
                                    while (answersResultSet.next()) {
                                        System.out.println("--------------------------------------------------------------------");
                                        System.out.println("question # " + i++);
                                        System.out.println("Your answer: " + answersResultSet.getString(3));
                                        if (answersResultSet.getBoolean(4)) {
                                            System.out.println("IS CORRECT");
                                        } else {
                                            System.out.println("FALSE");
                                        }
                                    }
                                    System.out.println("--------------------------------------------------------------------");
                                    Formatter formatter = new Formatter();
                                    formatter.format("RESULT: %.2f%%", result);
                                    System.out.println(formatter);
                                    formatter.close();
                                    answersStatement.close();
                                } else {
                                    System.out.println("Sorry, but you have no access rights to watch this attempt.");
                                    System.out.println("Try another attemt id.");
                                }
                                checkStatement.close();
                            } else {  //Если введенного id попытки сдачи теста вообще не сущестует в базе данных
                                System.out.println("There is no registered attempts with such id");
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("Incorrect input!");
                        }
                    }
                }
                break;
            }
            if (choice != EXIT_MODE) {
                Service.pressEnterToContinue();
            }
        } while (choice != EXIT_MODE);
    }

    //Процедура прохождения теста
    private static void tryTest(String login, Connection connection, Properties properties) throws SQLException {
        int correctAnswers = 0;
        int questionsCount = 0;
        Service.cleanConsole();
        Statement statementForTopics = connection.createStatement();
        ResultSet topics = select(statementForTopics, "*", (String) properties.get(TOPICS_TABLE));  //Получаем список всех категорий вопросов
        String line = (String) properties.get(QUESTIONS_TO_ASK_COUNT);//Заданное кол-во вопросов, которые надо задать в каждой категории
        int count = Integer.parseInt(line);

        ArrayList<Integer> allQuestionsId = new ArrayList<>(); //Тут будут записаны все id вопросов теста для помещения в историю
        ArrayList<String> allAnswers = new ArrayList<>();   //Тут будут записаны все ответы теста для помещения в историю
        ArrayList<Boolean> isCorrectMarks = new ArrayList<>(); //Тут будут записаны все оценки "верно-неверно"

        //Перебираем все категории вопросов
        while (topics.next()) {
            int topicId = topics.getInt((String) properties.get(TOPIC_ID_FIELD));
            //Находим список вопросов из текущей категории
            Statement statement = connection.createStatement();
            ResultSet questions = selectWithIntRestraint(statement, "*",
                    (String) properties.get(QUESTIONS_TABLE),
                    (String) properties.get(TOPIC_ID_FIELD), topicId);
            ArrayList<Integer> questionsIdList = new ArrayList<>(); //Список id вопросов текущей категории
            //Перебираем все вопросы текущей категории и фиксируем их id в questionsIdList
            while (questions.next()) {
                int questionId = questions.getInt((String) properties.get(QUESTION_ID_FIELD));
                questionsIdList.add(questionId);
            }
            statement.close();
            count = count < questionsIdList.size() ? count : questionsIdList.size();  //Мы не можем задать вопросов больше чем их есть в категории
            int[] randomNumbers = randomSampling(questionsIdList, count);   //Сюда запишем случайные id вопросов из текущей категории
            //Задаем в случайном порядке заданное количество вопросов из текущей категории
            for (int id : randomNumbers) {
                statement = connection.createStatement();
                ResultSet questionToAsk = selectWithIntRestraint(statement,
                        (String) properties.get(QUESTION_FIELD),
                        (String) properties.get(QUESTIONS_TABLE),
                        (String) properties.get(QUESTION_ID_FIELD), id);
                if (questionToAsk.next()) {
                    String question = questionToAsk.getString((String) properties.get(QUESTION_FIELD));
                    System.out.println(question);   //Задаем вопрос
                    //Находим список возможных ответов к вопросу
                    Statement statementForAnswers = connection.createStatement();
                    ResultSet probableAnswers = selectWithIntRestraint(statementForAnswers, "*",
                            (String) properties.get(PROBABLE_ANSWERS_TABLE),
                            (String) properties.get(QUESTION_ID_FIELD), id);
                    ArrayList<String> answers = new ArrayList<>();
                    ArrayList<Boolean> cheatList = new ArrayList<>();
                    int index = 0;
                    while (probableAnswers.next()) {
                        String answer = probableAnswers.getString((String) properties.get(ANSWER_FIELD));
                        Boolean isCorrect = probableAnswers.getBoolean((String) properties.get(IS_CORRECT_FIELD));
                        answers.add(answer);
                        cheatList.add(isCorrect);
                        System.out.println(index++ + " - " + answer);
                    }
                    statementForAnswers.close();

                    System.out.print("Your answer:");
                    index = Service.getIntegerBounded(0, answers.size() - 1);

                    allQuestionsId.add(id);
                    allAnswers.add(answers.get(index));
                    isCorrectMarks.add(cheatList.get(index));

                    questionsCount++;
                    if (cheatList.get(index)) {
                        correctAnswers++;
                    }
                } else {
                    System.out.println("Question was not found");   //Это сообщение никогда не должно появиться
                }
                statement.close();
            }
        }
        statementForTopics.close();
        double result = (((double) correctAnswers) / questionsCount) * 100;
        Formatter formatter = new Formatter();
        formatter.format("The test is over. Your result is %.2f%%", result);
        System.out.println(formatter);
        formatter.close();

        //Записываем результат прохождения теста в историю
        int attempt_id = insertAttempt(connection, properties, (String) properties.get(ATTEMPS_TABLE), login, result);
        for (int i = 0; i < allQuestionsId.size(); i++) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("INSERT INTO given_answers (" + properties.get(QUESTION_ID_FIELD) +
                    ", " + properties.get(GIVEN_ANSWER_FIELD) + ", " +
                    properties.get(IS_CORRECT_FIELD) + " , " + properties.get(ATTEMPT_ID_FIELD) + ") " +
                    "VALUES (" + allQuestionsId.get(i) + ", '" + allAnswers.get(i) + "', " + isCorrectMarks.get(i) + ", " + attempt_id + ")");
            statement.close();
        }
    }

    //Метод в случайном порядке отбирает из коллекции заданное количество элементов
    private static int[] randomSampling(ArrayList<Integer> questionsIdList, int count) {
        Random rnd = new Random();
        if (questionsIdList.size() >= count) {
            int[] result = new int[count];
            //За каждый проход цикла добавляем один случайный номер из коллекции questionsIdList, контролируя при этом
            //то, чтобы один и тот же номер включался в результирующий массив не больше одного раза
            for (int i = 0; i < count; i++) {
                boolean newNumber;
                int number;
                do {
                    newNumber = true;
                    int index = rnd.nextInt(questionsIdList.size());    //Порядковый номер элемента из коллекции
                    number = questionsIdList.get(index);    //id вопроса, который будет включен в выборку (если он еще не включен)
                    for (int j = 0; j < i; j++) {
                        if (result[j] == number) {
                            newNumber = false;
                            break;
                        }
                    }
                } while (!newNumber);
                result[i] = number; //Вопрос с id == number попал в выборку и будет задан пользователю
            }
            return result;
        } else {
            //Размер коллекции меньше чем размер выборки, которую надо из него получить
            throw new IllegalArgumentException("Array size is smaller then size of required sampling");
        }
    }
}