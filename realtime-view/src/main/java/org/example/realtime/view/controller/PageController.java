package org.example.realtime.view.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {
    
    /**
     * 仪表板页面
     */
    @GetMapping("/")
    public String dashboard() {
        return "dashboard";
    }
}