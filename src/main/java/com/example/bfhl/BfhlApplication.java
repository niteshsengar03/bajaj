package com.example.bfhl;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class BfhlApplication implements CommandLineRunner {

    private static final String GENERATE_WEBHOOK_URL =
            "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

    public static void main(String[] args) {
        SpringApplication.run(BfhlApplication.class, args);
    }

    @Override
    public void run(String... args) {
        try {
            executeFlow();
        } catch (Exception e) {
            System.err.println("Error during assignment flow: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void executeFlow() {
        // Local RestTemplate instance – no Spring bean
        RestTemplate restTemplate = new RestTemplate();

        // 1. Call generateWebhook with your correct details
        GenerateWebhookRequest requestBody = new GenerateWebhookRequest(
                "Nitesh Singh",
                "22BLC1017",
                "nitesh.singh2022@vitstudent.ac.in"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<GenerateWebhookRequest> entity =
                new HttpEntity<>(requestBody, headers);

        ResponseEntity<GenerateWebhookResponse> response =
                restTemplate.postForEntity(
                        GENERATE_WEBHOOK_URL,
                        entity,
                        GenerateWebhookResponse.class
                );

        if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
            throw new IllegalStateException("Failed to generate webhook: " + response.getStatusCode());
        }

        GenerateWebhookResponse body = response.getBody();
        String webhookUrl = body.getWebhook();
        String accessToken = body.getAccessToken();

        System.out.println("Received webhook: " + webhookUrl);
        System.out.println("Received accessToken: " + accessToken);

        // 2. Prepare final SQL query
        String finalQuery =
                "WITH filtered_payments AS (\n" +
                "    SELECT \n" +
                "        p.EMP_ID,\n" +
                "        SUM(p.AMOUNT) AS total_salary\n" +
                "    FROM PAYMENTS p\n" +
                "    WHERE EXTRACT(DAY FROM p.PAYMENT_TIME) <> 1\n" +
                "    GROUP BY p.EMP_ID\n" +
                "),\n" +
                "ranked AS (\n" +
                "    SELECT \n" +
                "        d.DEPARTMENT_NAME,\n" +
                "        fp.total_salary AS SALARY,\n" +
                "        CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME,\n" +
                "        DATE_PART('year', AGE(CURRENT_DATE, e.DOB)) AS AGE,\n" +
                "        ROW_NUMBER() OVER (\n" +
                "            PARTITION BY d.DEPARTMENT_ID\n" +
                "            ORDER BY fp.total_salary DESC\n" +
                "        ) AS rn\n" +
                "    FROM filtered_payments fp\n" +
                "    JOIN EMPLOYEE e \n" +
                "        ON e.EMP_ID = fp.EMP_ID\n" +
                "    JOIN DEPARTMENT d \n" +
                "        ON d.DEPARTMENT_ID = e.DEPARTMENT\n" +
                ")\n" +
                "SELECT \n" +
                "    DEPARTMENT_NAME,\n" +
                "    SALARY,\n" +
                "    EMPLOYEE_NAME,\n" +
                "    AGE\n" +
                "FROM ranked\n" +
                "WHERE rn = 1\n" +
                "ORDER BY DEPARTMENT_NAME;";

        // 3. Submit final query to webhook
        Map<String, String> submitBody = new HashMap<>();
        submitBody.put("finalQuery", finalQuery);

        HttpHeaders submitHeaders = new HttpHeaders();
        submitHeaders.setContentType(MediaType.APPLICATION_JSON);
        submitHeaders.set("Authorization", accessToken); // just token, no "Bearer"

        HttpEntity<Map<String, String>> submitEntity =
                new HttpEntity<>(submitBody, submitHeaders);

        ResponseEntity<String> submitResponse =
                restTemplate.postForEntity(
                        webhookUrl,
                        submitEntity,
                        String.class
                );

        System.out.println("Submit response status: " + submitResponse.getStatusCode());
        System.out.println("Submit response body: " + submitResponse.getBody());
    }
}
