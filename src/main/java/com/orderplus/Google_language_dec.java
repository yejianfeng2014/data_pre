package com.orderplus;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yejianfeng on 2018/11/5.
 * <p>
 * 这个是翻译功能，但是我只是用这个翻译里面的语言检测功能
 * 语言检测在翻译功能内嵌里面了,所以多了一个翻译过程
 */
public class Google_language_dec {

    public static String getLanguageType(String input) {

        String ret = "";
        String from = "auto";
        String to = "en";
        String q = input;
        String url = "http://translate.google.cn/translate_a/single";

        String tk = token(q);

        Map<String, String> params = new HashMap<String, String>();
        params.put("client", "t");
        params.put("sl", from);
        params.put("tl", to);
        params.put("hl", "zh-CN");
        params.put("dt", "at");
        params.put("dt", "bd");
        params.put("dt", "ex");
        params.put("dt", "ld");
        params.put("dt", "md");
        params.put("dt", "qca");
        params.put("dt", "rw");
        params.put("dt", "rm");
        params.put("dt", "ss");
        params.put("dt", "t");
        params.put("ie", "UTF-8");
        params.put("oe", "UTF-8");
        params.put("source", "btn");
        params.put("ssel", "0");
        params.put("tsel", "0");
        params.put("kc", "0");
        params.put("tk", tk);
        params.put("q", q);

        CloseableHttpClient httpClient = HttpClients.createDefault();

        CloseableHttpResponse response = null;
        HttpEntity entity = null;


        try {
            URIBuilder uri = new URIBuilder(url);
            for (String key : params.keySet()) {
                String value = params.get(key);
                uri.addParameter(key, value);
            }
            HttpUriRequest request = new HttpGet(uri.toString());
            response = httpClient.execute(request);
            entity = response.getEntity();

            String result = EntityUtils.toString(entity, "utf-8");
            JSONArray objects = JSON.parseArray(result);

            JSONArray jsonArray = objects.getJSONArray(8);

            JSONArray jsonArray1 = jsonArray.getJSONArray(3);
            String string1 = jsonArray1.getString(0);
            System.out.println(string1);

            EntityUtils.consume(entity);
            response.getEntity().getContent().close();
            response.close();


            return string1;
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            if (response != null) {

                try {
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return ret;
    }

    private static String token(String value) {
        String result = "";

        ScriptEngine engine = new ScriptEngineManager().getEngineByName("js");
        try {
            FileReader reader = new FileReader("D:\\spingboot\\src\\main\\resources\\Google.js");
            engine.eval(reader);

            if (engine instanceof Invocable) {
                Invocable invoke = (Invocable) engine;
                result = String.valueOf(invoke.invokeFunction("token", value));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


}
