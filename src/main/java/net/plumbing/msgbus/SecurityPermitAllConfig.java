package net.plumbing.msgbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

@Configuration
@EnableWebSecurity

public  class SecurityPermitAllConfig // extends WebSecurityConfigurerAdapter 
        implements WebMvcConfigurer {

    private static final Logger WebSecurityConfig_log = LoggerFactory.getLogger(SecurityPermitAllConfig.class);
    @Autowired
    private MyBasicAuthenticationEntryPoint authenticationEntryPoint;

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
        auth.inMemoryAuthentication()
                .withUser("user123").password(passwordEncoder().encode("user123Pass"))
                .authorities("ROLE_USER");
        WebSecurityConfig_log.warn("configureGlobal: auth.inMemoryAuthentication().withUser().authorities" +
                "() DONE!");
    }

    @Bean
    public WebSecurityCustomizer webSecurityCustomizer() {
        WebSecurityConfig_log.warn("webSecurityCustomizer: (web) -> web.ignoring().requestMatchers(new AntPathRequestMatcher(\"/**\")" +
                "() DONE!");
        return (web) -> web.ignoring()
                .requestMatchers(new AntPathRequestMatcher("/**"));

    }
    @Bean
    //@Autowired
    public SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http) throws Exception {
        http
                // .csrf().disable not recommended in prod environment
                .csrf((csrf) -> csrf.disable())
                .authorizeHttpRequests((authz) -> authz
                        .requestMatchers("/**").permitAll()
                        .anyRequest().authenticated()
                );
        WebSecurityConfig_log.warn("defaultSecurityFilterChain: authorizeHttpRequests(authz) -> authz).requestMatchers(\"/**\").permitAll().anyRequest().authenticated()" +
                "() DONE!");
        return http.build();
    }

//    public class CsrfSecurityConfig {
//
//        @Bean
//        public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
//            http
//                    .csrf((csrf) -> csrf.disable());
//            return http.build();
//        }
//    }
    //@Override
    protected void configure(HttpSecurity http) throws Exception {
        //http.authorizeRequests().antMatchers("/HermesService/InternalRestApi/**").authenticated().and().httpBasic();
       http.authorizeRequests().anyRequest().permitAll(); //.and().csrf().disable();

        //http.authorizeRequests().anyRequest().authenticated().and().httpBasic();


        //!java.lang.StackOverflowError: null!  http.authorizeRequests().antMatchers("/HermesService/InternalRestApi/**").authenticated().and().httpBasic().authenticationEntryPoint(authenticationEntryPoint);
        WebSecurityConfig_log.warn("Configure: http.csrf().disable().authorizeRequests().anyRequest().permitAll() DONE!");
    }
    ///?
   // @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(this.userDetailsService())
               // .and().eraseCredentials(false   )
        ;
        WebSecurityConfig_log.warn("Configure: AuthenticationManagerBuilder() DONE!");
    }

    private UserDetailsService userDetailsService() {
        UserDetails user = User.withDefaultPasswordEncoder()
                .username("user123")
                .password("user123Pass")
                .roles("USER")
                .build();
        return new InMemoryUserDetailsManager(user);
       // return null;
    }


    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }
    ///--------------------------
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**").allowedMethods("*");
        WebSecurityConfig_log.warn( "registry.addMapping(\"/**\")" );
    }
    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Arrays.asList("*"));
        configuration.setAllowedMethods(Arrays.asList("*"));
        configuration.setAllowedHeaders(Arrays.asList("*"));
        configuration.setAllowCredentials(true);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/HermesService/InternalRestApi/apiSQLRequest/**", configuration);
        WebSecurityConfig_log.warn("CorsConfigurationSource, setAllowedOrigins: setAllowedOrigins, setAllowedMethods, setAllowedHeaders, setAllowCredentials:  DONE!");
        return source;
    }
}