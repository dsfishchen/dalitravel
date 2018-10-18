package com.dalitravel.bigdataservice.utils;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.security.Key;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JavaWebToken {

    private static Logger log = LoggerFactory.getLogger(JavaWebToken.class);
    public static final int calendarField = Calendar.DATE;
    // 设置token过期的时间间隔常量，一天为单位
    public static final int calendarInterval = 3;


    //该方法使用HS256算法和Secret:yuncaibang生成signKey
    private static Key getKeyInstance() {
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
        byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary("bigdata");
        Key signingKey = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());
        return signingKey;
    }

    //使用HS256签名算法和生成的signingKey最终的Token,claims中是有效载荷
    public static String createJavaWebToken(Map<String, Object> claims) {
        Date iatDate = new Date();
        Calendar nowTime = Calendar.getInstance();
        nowTime.add(calendarField, calendarInterval);
        Date expiresDate = nowTime.getTime();
        Map<String, Object> map = new HashMap<>();
        map.put("alg", "HS256");
        map.put("typ", "JWT");

        String token=Jwts.builder().
                setHeader(map).
                setClaims(claims).
                setIssuedAt(iatDate).
                setExpiration(expiresDate).
                signWith(SignatureAlgorithm.HS256, getKeyInstance()).compact();
        return token;
    }

    //解析Token，同时也能验证Token，当验证失败返回null
    public static Map<String, Object> parserJavaWebToken(String jwt) {
        try {
            Map<String, Object> jwtClaims =
                    Jwts.parser().setSigningKey(getKeyInstance()).parseClaimsJws(jwt).getBody();
            return jwtClaims;
        } catch (Exception e) {
            log.error("token失效");
            return null;
        }
    }

  //根据token，返回登录的商户的ID
    public  static  String  getStringId(String jwt){
        Map<String, Object> map=parserJavaWebToken(jwt);
        String supplierId="";
        if(map!=null){
           supplierId= (String) map.get("supplierId");
        }
        return supplierId;
    }

    public  static  void main(String[] arg){
     String str="[/upload/article/c39c3d7b-26f2-430b-ae3c-58893c2d3240.png, /upload/article/629c9848-8d56-4442-ae2f-1e45481ea674.png, /upload/article/a2e8a7fd-f40e-4a7d-ba92-d922a71fd301.png]";
     System.out.println(str.replace("[]",""));
    }
}
