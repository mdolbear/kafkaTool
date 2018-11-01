package com.mjdsoftware.kafkatool.exceptions;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 *
 */
@Slf4j
public class KafkaToolExceptionTranslator extends ResponseEntityExceptionHandler {


    /**
     * Answer my logger
     * @return Logger
     */
    private static Logger getLogger() {

        return log;
    }

    /**
     * Answer a default instance
     */
    public KafkaToolExceptionTranslator() {

        super();

    }

    /**
     * Handle IllegalStateException
     */
    @ExceptionHandler(value = {IllegalStateException.class,
                               IllegalArgumentException.class,
                               NullPointerException.class,
                               Exception.class})
    public ResponseEntity<Object> handleConflict(RuntimeException anException,
                                                 WebRequest aRequest) {

        String  tempMsg;

        tempMsg = anException.getMessage();
        getLogger().error(tempMsg);

        return this.handleExceptionInternal(anException,
                                            new ErrorMessage(HttpStatus.BAD_REQUEST, tempMsg),
                                            new HttpHeaders(),
                                            HttpStatus.BAD_REQUEST,
                                            aRequest);

    }

}
