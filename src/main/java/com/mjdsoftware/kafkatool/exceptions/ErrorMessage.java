package com.mjdsoftware.kafkatool.exceptions;

import org.springframework.http.HttpStatus;

/**
 *
 */
public class ErrorMessage {


    private HttpStatus status;
    private String message;

    /**
     * Answer an instance for the following arguments
     */
    public ErrorMessage() {
        super();
    }

    /**
     * Answer an instance for the following arguments
     * @param aStatus HttpStatus
     * @param aMessage String
     */
    public ErrorMessage(HttpStatus aStatus,
                        String aMessage) {

        this();
        this.setStatus(aStatus);
        this.setMessage(aMessage);
    }

    /**
     * Answer my status
     * @return HttpStatus
     */
    public HttpStatus getStatus() {
        return status;
    }

    /**
     * Set my status
     * @param status HttpStatus
     */
    public void setStatus(HttpStatus status) {
        this.status = status;
    }

    /**
     * Answer my message
     * @return String
     */
    public String getMessage() {
        return message;
    }

    /**
     * Set my message
     * @param message String
     */
    public void setMessage(String message) {
        this.message = message;
    }
}
