package com.github.elizalex.grabber.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.*;
import java.util.Properties;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

/**
 * 
 */
public class AlertRabbit {

    private static Properties properties;

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
     * Класс содержит задание (работу), которое выполняется при срабатывании триггера
     */
    public static class Rabbit implements Job {

        /**
         * Метод содержит задание, которое выполняется при вызове метода.
         * Вызывается планировщиком при срабатывании определенного триггера.
         * @param context параметры, информация о состоянии для экземпляров задания
         * @throws JobExecutionException - если при выполнении задания возникла
         * исключительная ситуация
         */
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            System.out.println("Rabbit runs here ...");
        }
    }

    public static void main(String[] args) {
        loadProperties("rabbit.properties"); // считываем данные properties файла
        int interval = Integer.parseInt(properties.getProperty("rabbit.interval")); //для удобства
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler(); // создаем планировщик
            scheduler.start(); // запуск планировщика
            JobDetail job = newJob(Rabbit.class).build(); // постановка задачи
            SimpleScheduleBuilder times = simpleSchedule() // задается расписание
                    .withIntervalInSeconds(interval) //интервал для триггера
                    .repeatForever(); // бесконечное повторение триггера
            Trigger trigger = newTrigger() // задается триггер для выполнения задачи
                    .startNow() // запуск отсчета интервала с момента запуска программы
                    .withSchedule(times) // интервал срабатывания триггера (расписание выполнения)
                    .build(); // сборка триггера
            scheduler.scheduleJob(job, trigger); // добавляет задачу и триггер в планировщик
        } catch (SchedulerException se) {
            se.printStackTrace(); // отображает трассировку стека при исключении
        }
    }
}
