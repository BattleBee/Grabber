package com.github.elizalex.grabber.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

public class AlertRabbitDB {

    private static Properties properties;
    private static Connection connection;

    /**
     * Считывает конфиг. файл
     */
    public static void loadProperties(String propertiesFileName) {
        properties = new Properties();
        try (InputStream in =
                     AlertRabbit.class.getClassLoader().getResourceAsStream(propertiesFileName)) {
            properties.load(in);
        } catch (IOException ex) {
            System.out.println("Ошибка чтения конфигурационного файла: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * Создает подключение к БД
     */
    private static void initConnection() {
        try {
            Class.forName(properties.getProperty("driver"));
            String url = properties.getProperty("url");
            String login = properties.getProperty("username");
            String password = properties.getProperty("password");
            connection = DriverManager.getConnection(url, login, password);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * Класс содержит задание (работу), которое выполняется при срабатывании триггера
     */
    public static class Rabbit implements Job {

        public Rabbit() {
            System.out.println(hashCode());
        }

        /**
         * Метод содержит задание, которое необходимо выполнить.
         * Вызывается планировщиком при срабатывании  триггера, связанного с заданием
         * @param context параметры, свойства, условия выполнения экземпляров задания
         * @throws JobExecutionException - если при выполнении задания возникла
         * исключительная ситуация
         */
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            System.out.println("Rabbit runs here ...");
            // Чтобы получить connection из context используется следующий вызов.
            Connection connection = (Connection) context.getJobDetail() // получаем JobDetail
                    .getJobDataMap() // // получаем объект JobDataMap связанный с заданием
                    .get("connection"); // получаем объект connection - соединение с БД.
            try (Statement statement = connection.createStatement()) { // создаем запрос в БД
                statement.execute(
                        "insert into rabbit (created_date) values (current_timestamp)"
                ); // добавляем значения в таблицу  БД
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        loadProperties("rabbit.properties"); // считываем данные properties файла
        int interval = Integer.parseInt(properties.getProperty("rabbit.interval")); //для удобства
        initConnection(); // создаем соединение с БД
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler(); // создаем планировщик
        scheduler.start(); // запуск планировщика
        JobDataMap data = new JobDataMap(); // содержит инфо о состоянии для экземпляров задания
        data.put("connection", connection); // добавляет данные о соединении в карту данных задания
        JobDetail job = newJob(Rabbit.class) // постановка задачи
                .usingJobData(data) // добавление данных из JobDataMap data
                .build(); // сборка задачи
        SimpleScheduleBuilder times = simpleSchedule() // задатся рассписание
                .withIntervalInSeconds(interval) // установка интервала для триггера
                .repeatForever(); // бесконечное повторение триггера
        Trigger trigger = newTrigger() // задается триггер для выполнения задачи
                .startNow() // начало запуска отсчета интервала
                .withSchedule(times) // интервал между выполнением (рассписание выполнения)
                .build(); // сборка триггера
        scheduler.scheduleJob(job, trigger); // добавляет задачу и триггер в планировщик
        Thread.sleep(10000); //пауза на 10 секунд перед завершающей командой
        scheduler.shutdown(); //останавливает триггеры и очищает ресурсы связанные с планировщиком
    }
}
